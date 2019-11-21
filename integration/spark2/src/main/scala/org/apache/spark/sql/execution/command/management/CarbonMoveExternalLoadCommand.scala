/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command.management

import java.io.{File, IOException}
import java.util

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableMergeIndexEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreExecutionEvent
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.execution.command.{AlterTableModel, Checker, DataCommand}
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Move specified external segments to internal.
 * This operation will copy all files of specified external segment to a newly created internal segment, and
 * perform index merge for the new segment.
 */
case class CarbonMoveExternalLoadCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String])
  extends DataCommand {

  // validate input options, return value of 'start_from' and 'number'
  private def validateOptions(options: Map[String, String]): (Int, Int) = {
    val startFromOp = options.get("start_from")
    if (startFromOp.isEmpty) {
      throw new MalformedCarbonCommandException("'start_from' option must be specified")
    }
    val startFrom = try {
      Integer.valueOf(startFromOp.get)
    } catch {
      case e: NumberFormatException =>
        throw new MalformedCarbonCommandException(s"'start_from' option should be integer: " + startFromOp.get)
    }
    val numSegmentsToMoveOp = options.get("number")
    if (numSegmentsToMoveOp.isEmpty) {
      throw new MalformedCarbonCommandException("'number' option must be specified")
    }
    val numSegmentsToMove = try {
      Integer.valueOf(numSegmentsToMoveOp.get)
    } catch {
      case e: NumberFormatException =>
        throw new MalformedCarbonCommandException(s"'number' option should be integer: " + numSegmentsToMoveOp.get)
    }
    if (numSegmentsToMove <= 0) {
      throw new MalformedCarbonCommandException(s"'number' option should be greater than 0: " + numSegmentsToMoveOp.get)
    }
    (startFrom, numSegmentsToMove)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // table should exist
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // validate input options
    val (startSegment, numSegmentsToMove) = validateOptions(options)
    val endSegment = startSegment + numSegmentsToMove - 1

    // if insert overwrite in progress, do not allow move segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "move segment")
    }

    // For all segments specified by user, check whether they are valid to move
    val loadDetails = SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val metas = (startSegment to endSegment).map { segmentName =>
      val loadDetail = loadDetails.find(_.getLoadName.equalsIgnoreCase(String.valueOf(segmentName)))
        .getOrElse(throw new AnalysisException(s"Segment name $segmentName doesn't exist"))
      if (StringUtils.isEmpty(loadDetail.getPath)) {
        throw new AnalysisException(s"Segment $segmentName is not a external segment")
      }
      if (!(loadDetail.getSegmentStatus == SegmentStatus.SUCCESS ||
        loadDetail.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS)) {
        throw new AnalysisException(s"Segment $segmentName status is ${loadDetail.getSegmentStatus}")
      }
      // here we read the segment file into driver's memory
      // TODO: move to executor if number of segment to move is too large, in order to avoid bottlenet in driver
      (loadDetail, new SegmentFileStore(carbonTable.getTablePath, loadDetail.getSegmentFile))
    }

    // check whether it is ok to start copying by sending event
    val destSegmentId = String.valueOf(SegmentStatusManager.createNewSegmentId(loadDetails))
    val destSegmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, destSegmentId)
    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)
    fireEventBeforeLoading(model, carbonTable, sparkSession, destSegmentPath)

    // metas contains all load detail and segment file metadata
    // collect all files to copy and launch a spark job to copy all files distributedly
    val filesToCopy = new ArrayBuffer[String]()
    metas.foreach { case (loadDetail, segmentFile) =>
      filesToCopy ++=
        FileFactory.getCarbonFile(loadDetail.getPath)
          .listFiles(false)
          .asScala
          .map(_.getAbsolutePath)
    }

    // create folder for destination segment
    FileFactory.mkdirs(destSegmentPath, sparkSession.sessionState.newHadoopConf())

    // copy all files to destination segment by launching spark job
    import sparkSession.implicits._
    val ds = filesToCopy.toDS.map { srcFilePath =>
      val srcFile = FileFactory.getCarbonFile(srcFilePath)
      srcFile.copyTo(destSegmentPath)
    }.groupByKey(copySuccessOrNot => copySuccessOrNot)
    .count()

    val result: Map[Boolean, Long] = ds.collect().toMap
    if (result.get(false).nonEmpty) {
      // at least one copy is failed, clean up and make the whole operation failed
      // TODO: time consuming?
      FileFactory.deleteAllFilesOfDir(new File(destSegmentPath))
      throw new RuntimeException(result.get(false) + " file(s) copy failed")
    } else {
      // all copies are success, do following:
      // 1. merge all index files by sending event
      // 2. write a new segment file
      // 3. update table status file (remove copied entry and add new entry)
      // 4. delete all copied files by launching spark job

      val alterTableModel = AlterTableModel(
        dbName = Some(carbonTable.getDatabaseName),
        tableName = carbonTable.getTableName,
        segmentUpdateStatusManager = None,
        compactionType = "",
        factTimeStamp = Some(System.currentTimeMillis()),
        alterSql = null,
        customSegmentIds = Some(Seq(destSegmentId).toList))
      val mergeIndexEvent = AlterTableMergeIndexEvent(sparkSession, carbonTable, alterTableModel)
      OperationListenerBus.getInstance().fireEvent(mergeIndexEvent, new OperationContext)

      try {
        val destSegmentFile = new Segment(
          destSegmentId,
          SegmentFileStore.genSegmentFileName(destSegmentId, System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
          destSegmentPath,
          new util.HashMap[String, String]())
        val isSuccess = SegmentFileStore.writeSegmentFile(carbonTable, destSegmentFile)
        if (isSuccess) {
          var dataSize = 0
          var indexSize = 0
          val updatedLoadDetails = new ArrayBuffer[LoadMetadataDetails]()
          loadDetails.map { loadDetail =>
            val segmentId = Integer.valueOf(loadDetail.getLoadName)
            if (segmentId >= startSegment && segmentId <= endSegment) {
              loadDetail.setSegmentStatus(SegmentStatus.COMPACTED)
              dataSize = dataSize + Integer.valueOf(loadDetail.getDataSize)
              indexSize = indexSize + Integer.valueOf(loadDetail.getIndexSize)
            }
            updatedLoadDetails += loadDetail
          }
          val newLoadMetaEntry = new LoadMetadataDetails
          newLoadMetaEntry.setSegmentStatus(SegmentStatus.SUCCESS)
          newLoadMetaEntry.setLoadStartTime(System.currentTimeMillis())
          newLoadMetaEntry.setLoadName(destSegmentId)
          newLoadMetaEntry.setPath(destSegmentPath)
          newLoadMetaEntry.setSegmentFile(destSegmentFile.getSegmentFileName)
          newLoadMetaEntry.setDataSize(String.valueOf(dataSize))
          newLoadMetaEntry.setIndexSize(String.valueOf(indexSize))
          updatedLoadDetails += newLoadMetaEntry
          SegmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath),
            updatedLoadDetails.toArray)
        } else {
          FileFactory.deleteAllFilesOfDir(new File(destSegmentPath))
          throw new RuntimeException("Write segment file failed: " + destSegmentFile)
        }
      } catch {
        case ex: IOException =>
          FileFactory.deleteAllFilesOfDir(new File(destSegmentPath))
          throw ex
      }
      // TODO: delete all old files, time consuming?
      // FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location, configuration))
    }
    Seq.empty
  }

  private def fireEventBeforeLoading(model: CarbonLoadModel, carbonTable: CarbonTable,
      sparkSession: SparkSession, segmentPath: String): Unit = {
    val operationContext = new OperationContext
    val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
      new LoadTablePreExecutionEvent(carbonTable.getCarbonTableIdentifier, model)
    operationContext.setProperty("isOverwrite", false)
    OperationListenerBus.getInstance.fireEvent(loadTablePreExecutionEvent, operationContext)
    // Add pre event listener for index datamap
    val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable)
    val dataMapOperationContext = new OperationContext()
    if (tableDataMaps.size() > 0) {
      val dataMapNames = tableDataMaps.asScala.map(_.getDataMapSchema.getDataMapName)
      val event = BuildDataMapPreExecutionEvent(sparkSession, carbonTable.getAbsoluteTableIdentifier, dataMapNames)
      OperationListenerBus.getInstance().fireEvent(event, dataMapOperationContext)
    }
  }

  override protected def opName: String = "MOVE SEGMENT"
}
