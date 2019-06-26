/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.command

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.{AlterTableModel, AtomicRunnableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.{CarbonInternalMergerUtil, CarbonInternalScalaUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{LoadTableSIPostExecutionEvent, LoadTableSIPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil
import org.apache.carbondata.spark.spark.util.CarbonPluginUtil

case class SIRebuildSegmentCommand(
  alterTableModel: AlterTableModel,
  tableInfoOp: Option[TableInfo] = None,
  var rebuildSIsql: String = null,
  operationContext: OperationContext = new OperationContext)
  extends AtomicRunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  var indexTable: CarbonTable = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableModel.tableName.toLowerCase
    val dbName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    indexTable = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
      relation.carbonTable
    }
    setAuditTable(indexTable)
    if (!indexTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (!CarbonInternalScalaUtil.isIndexTable(indexTable)) {
      throw new UnsupportedOperationException("Unsupported operation on carbon table")
    }

    val version = CarbonUtil.getFormatVersion(indexTable)
    val isOlderVersion = version == ColumnarFormatVersion.V1 ||
                         version == ColumnarFormatVersion.V2
    if (isOlderVersion) {
      throw new MalformedCarbonCommandException(
        "Unsupported rebuild operation on carbon table: Merge data files is not supported on V1 " +
        "V2 store segments")
    }
    // check if the list of given segments in the command are valid
    val segmentIds: List[String] = {
      if (alterTableModel.customSegmentIds.isDefined) {
        alterTableModel.customSegmentIds.get
      } else {
        List.empty
      }
    }
    if (!segmentIds.isEmpty) {
      val segmentStatusManager = new SegmentStatusManager(indexTable.getAbsoluteTableIdentifier)
      val validSegments = (segmentStatusManager.getValidAndInvalidSegments.getValidSegments).asScala
        .map(_.getSegmentNo)
      segmentIds.foreach(segmentId =>
        if (!validSegments.contains(segmentId)) {
          throw new RuntimeException(s"Rebuild index by segment id is failed. " +
                                     s"Invalid ID: $segmentId")
        }
      )
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    LOGGER.info( s"SI segment compaction request received for table " +
                 s"${ indexTable.getDatabaseName}.${indexTable.getTableName}")
    val metaStore = CarbonEnv.getInstance(sparkSession)
      .carbonMetaStore
    val mainTable = metaStore
      .lookupRelation(Some(indexTable.getDatabaseName),
        CarbonInternalScalaUtil.getParentTableName(indexTable))(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
    val lock = CarbonLockFactory.getCarbonLockObj(
      mainTable.getAbsoluteTableIdentifier,
      LockUsage.COMPACTION_LOCK)

    var segmentList: List[String] = null
    val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
    var segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
      scala.collection.mutable.Map()

    var loadMetadataDetails: Array[LoadMetadataDetails] = null

    try {
      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
                    s" ${mainTable.getDatabaseName}.${mainTable.getTableName}")

        val operationContext = new OperationContext
        val loadTableSIPreExecutionEvent: LoadTableSIPreExecutionEvent =
          LoadTableSIPreExecutionEvent(sparkSession,
            new CarbonTableIdentifier(indexTable.getDatabaseName, indexTable.getTableName, ""),
            null,
            indexTable)
        OperationListenerBus.getInstance
          .fireEvent(loadTableSIPreExecutionEvent, operationContext)

        if (alterTableModel.customSegmentIds.isDefined) {
          segmentList = alterTableModel.customSegmentIds.get
        }

        SegmentStatusManager.readLoadMetadata(mainTable.getMetadataPath) collect ({
          case loadDetails if (null == segmentList ||
                               segmentList.contains(loadDetails.getLoadName)) =>
            segmentFileNameMap
              .put(loadDetails.getLoadName,
                String.valueOf(loadDetails.getLoadStartTime))
        })

        loadMetadataDetails = (SegmentStatusManager
          .readLoadMetadata(indexTable.getMetadataPath))
          .filter(loadMetadataDetail =>
            (null == segmentList || segmentList.contains(loadMetadataDetail.getLoadName)) &&
            (loadMetadataDetail.getSegmentStatus ==
             SegmentStatus.SUCCESS ||
             loadMetadataDetail.getSegmentStatus ==
             SegmentStatus.LOAD_PARTIAL_SUCCESS))

        segmentIdToLoadStartTimeMapping = CarbonInternalLoaderUtil
          .getSegmentToLoadStartTimeMapping(loadMetadataDetails)
          .asScala

        val carbonLoadModelForMergeDataFiles = CarbonInternalMergerUtil
          .getCarbonLoadModel(indexTable,
            loadMetadataDetails.toList.asJava,
            System.currentTimeMillis(), CarbonInternalScalaUtil
              .getCompressorForIndexTable(indexTable.getDatabaseName, indexTable.getTableName,
                mainTable.getTableName)(sparkSession))

        CarbonInternalMergerUtil
          .mergeDataFilesSISegments(segmentIdToLoadStartTimeMapping,
            indexTable,
            loadMetadataDetails.toList.asJava, carbonLoadModelForMergeDataFiles, true)(sparkSession
            .sqlContext)

        val loadTableACLPostExecutionEvent: LoadTableSIPostExecutionEvent =
          LoadTableSIPostExecutionEvent(sparkSession,
            indexTable.getCarbonTableIdentifier,
            null,
            indexTable)
        OperationListenerBus.getInstance
          .fireEvent(loadTableACLPostExecutionEvent, operationContext)

        LOGGER.info(s"SI segment compaction request completed for table " +
                    s"${indexTable.getDatabaseName}.${indexTable.getTableName}")
      } else {
        LOGGER.error(s"Not able to acquire the compaction lock for table" +
                     s" ${indexTable.getDatabaseName}.${indexTable.getTableName}")
        CarbonException.analysisException(
          "Table is already locked for compaction. Please try after some time.")
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"SI segment compaction request failed for table " +
                     s"${indexTable.getDatabaseName}.${indexTable.getTableName}")
      case ex: NoSuchTableException =>
        throw ex
    } finally {
      lock.unlock()
    }
    Seq.empty
  }

  override protected def opName: String = "SI Compact/Rebuild within segment"

}
