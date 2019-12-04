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

import java.io.IOException
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, ExecutorService}

import scala.collection.JavaConverters._

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.{StructField, StructType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager, StageInput}
import org.apache.carbondata.core.util.{CarbonUtil, DataFileFooterConverterV3}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.TableOptionConstant
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark

/**
 * collect loads from load_details folder
 */
case class CarbonCollectLoadsCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String]
) extends MetadataCommand {

  @transient var LOGGER: Logger = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val table = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    FileFactory.getConfiguration.addResource(hadoopConf)
    setAuditTable(table)
    if (!table.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    val batch = if (options.contains("batch")) {
      try {
        options("batch").toInt
      } catch {
        case e: Throwable =>
          throw new MalformedCarbonCommandException(
            s"'batch' option must be integer: ${options("batch")}")
      }
    } else {
      Int.MaxValue
    }
    if (batch <= 0) {
      throw new MalformedCarbonCommandException(
        s"'batch' option must be greater than 0: ${options("batch")}")
    }

    val tablePath = table.getTablePath
    val loadDetailsDir = CarbonTablePath.getLoadDetailsDir(tablePath)
    val snapshotFilePath = CarbonTablePath.getStageSnapshotFile(tablePath)

    acquiredLock(table, hadoopConf)
    try {
      // 1. Check whether we need to recover from previous failure
      // We use a snapshot file to indicate whether there was failure in previous
      // ingest operation. A Snapshot file will be created when an ingest operation
      // starts and will be deleted only after the whole ingest operation is finished,
      // which includes two actions:
      //   1) action1: changing segment status to SUCCESS and
      //   2) action2: deleting all involved stage files.
      //
      // If one of these two actions is failed, the snapshot file will be exist, so
      // that recovery is needed.
      //
      // To do the recovery, do following steps:
      //   1) Check if corresponding segment in table status is SUCCESS,
      //      means deleting stage files had failed. So need to read the stage
      //      file list from the snapshot file and delete them again.
      //   2) Check if corresponding segment in table status is INSERT_IN_PROGRESS,
      //      means data loading had failed. So need to read the stage file list
      //      from the snapshot file and load again.
      recoverIfRequired(snapshotFilePath, table, hadoopConf)

      // get the list of all load detail file
      val allDetailFiles = listLoadDetails(loadDetailsDir, hadoopConf)
      // sort it by time and take first one batch
      val detailFilesToLoad = allDetailFiles
        .sortWith { case ((df1, s1), (df2, s2)) =>
          df1.getLastModifiedTime < df2.getLastModifiedTime
        }
        .take(batch)

      if (detailFilesToLoad.isEmpty) {
        LOGGER.warn("files not found under load_details, no input to load")
      } else {
        // 1. read load_details dir
        val numThreads = Math.min(Math.max(detailFilesToLoad.length, 1), 10)
        val executorService = Executors.newFixedThreadPool(numThreads)
        val detailList = Collections.synchronizedList(new util.ArrayList[LoadMetadataDetails]())
        readLoadDetailDir(executorService, detailFilesToLoad, detailList)

        // 2. read all segment files and index files to get location of data files
        //    then create stage input
        val (stageInputs, segmentFilePaths) =
          getStageInputs(table, executorService, detailList, hadoopConf)

        // 3. write stage input into a snapshot file (for recovery in case any failure
        // in loading or failed in deleting files)
        val gson = new Gson()
        val content = gson.toJson(stageInputs.toArray)
        FileFactory.writeFile(content, snapshotFilePath)

        // 4. perform data loading
        startLoading(sparkSession, table, stageInputs, options)

        // 5. delete segment files written by flink
        deleteTempSegmentFiles(executorService, segmentFilePaths)

        // 6. delete load_detals files written by flink
        deleteLoadDetailDir(executorService, detailFilesToLoad)

        // 7. delete the snapshot file
        FileFactory.getCarbonFile(snapshotFilePath).delete()
      }
      LOGGER.info("finished to collect segments")
      Seq.empty
    } catch {
      case ex: Throwable =>
        LOGGER.error("failed to collect segments", ex)
        throw ex
    } finally {
      releaseLock(table, hadoopConf)
    }
  }

  /**
   * Check whether there was failure in previous ingest process and try to recover
   */
  private def recoverIfRequired(
      snapshotFilePath: String,
      table: CarbonTable,
      conf: Configuration): Unit = {
    if (!FileFactory.isFileExist(snapshotFilePath)) {
      // everything is fine
      return
    }

    // something wrong, read the snapshot file and do recover steps
    // 1. check segment state in table status file
    // 2. If in SUCCESS state, delete all stage files read inn snapshot file
    // 3. If in IN_PROGRESS state, delete the entry in table status and load again
    LOGGER.warn(s"snapshot file found ($snapshotFilePath), start recovery process")
    val lines = FileFactory.readLinesInFile(snapshotFilePath, conf)
    if (lines.size() != 1) {
      throw new RuntimeException("Invalid snapshot file, " + lines.size() + " lines")
    }

    val gson = new Gson()
    val stageInputs = gson.fromJson(lines.get(0), classOf[Array[StageInput]])
    LOGGER.warn(s"Previous collect segment aborted, need manual recovery, " +
                s"${stageInputs.length} stage entries, see ${snapshotFilePath}")

    // TODO
    // the recovery process is not implemented
  }

  private def getStageInputs(
      table: CarbonTable,
      executorService: ExecutorService,
      detailList: util.List[LoadMetadataDetails],
      conf: Configuration): (Seq[StageInput], Seq[String]) = {
    LOGGER.info(s"start to collect input files for loading")
    val startTime = System.currentTimeMillis()

    val stageInputList = Collections.synchronizedList(new util.ArrayList[StageInput]())
    val segmentFilePaths = Collections.synchronizedList(new util.ArrayList[String]())

    detailList.asScala.map { detail =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          // read segment file to get path of index file
          val segmentFilePath =
            CarbonTablePath.getSegmentFilePath(table.getTablePath, detail.getSegmentFile)
          segmentFilePaths.add(segmentFilePath)
          val segment = SegmentFileStore.readSegmentFile(segmentFilePath)
          val entry = segment.getLocationMap.entrySet().iterator().next()
          val indexFileBasePath = entry.getKey
          val indexFiles = entry.getValue.getFiles.toArray
          if (indexFiles.length > 1) {
            throw new RuntimeException(s"invalid segment file: $segmentFilePath")
          }
          // read index file to get path and size of data file
          val indexFilePath =
            indexFileBasePath + CarbonCommonConstants.FILE_SEPARATOR + indexFiles(0)
          val reader = new DataFileFooterConverterV3(conf)
          val footers = reader.getIndexInfo(indexFilePath, null, true)
          val dataFiles = footers.asScala.map { footer =>
            val dataFileFullPath = footer.getBlockInfo.getTableBlockInfo.getFilePath
            val dataFileName = new Path(dataFileFullPath).getName
            val dataFileSize = footer.getBlockInfo.getTableBlockInfo.getBlockLength
            (dataFileName, java.lang.Long.valueOf(dataFileSize))
          }
          val stageInput = new StageInput(indexFileBasePath, dataFiles.toMap.asJava)
          stageInputList.add(stageInput)
        }
      })
    }.map(_.get())

    val entTime = System.currentTimeMillis()
    LOGGER.info(s"finished collect input files, ${stageInputList.size()} input files, " +
      s"${segmentFilePaths.size()} temp segment files, time taken ${entTime - startTime}ms")
    (stageInputList.asScala, segmentFilePaths.asScala)
  }

  /**
   * Start global sort loading
   */
  private def startLoading(
      spark: SparkSession,
      table: CarbonTable,
      stageInput: Seq[StageInput],
      options: Map[String, String]
  ): Unit = {
    val splits = stageInput.flatMap(_.createSplits().asScala)
    LOGGER.info(s"start to load ${splits.size} files into " +
                s"${table.getDatabaseName}.${table.getTableName}")
    val start = System.currentTimeMillis()
    val dataFrame = DataLoadProcessBuilderOnSpark.createInputDataFrame(spark, table, splits)
    val header = dataFrame.schema.fields.map(_.name).mkString(",")
    val loadCommand = CarbonLoadDataCommand(
      databaseNameOp = Some(table.getDatabaseName),
      tableName = table.getTableName,
      factPathFromUser = null,
      dimFilesPath = Seq(),
      options = options ++ Map("fileheader" -> header),
      isOverwriteTable = false,
      inputSqlString = null,
      dataFrame = Some(dataFrame),
      updateModel = None,
      tableInfoOp = None)
    loadCommand.run(spark)
    LOGGER.info(s"finish data loading, time taken ${System.currentTimeMillis() - start}ms")
  }

  /**
   * create CarbonLoadModel for global_sort
   */
  def createLoadModelForGlobalSort(
      sparkSession: SparkSession,
      carbonTable: CarbonTable
  ): CarbonLoadModel = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    CarbonTableOutputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonTableOutputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonTableOutputFormat.setCarbonTable(conf, carbonTable)
    val fieldList = carbonTable.getCreateOrderColumn(carbonTable.getTableName)
      .asScala
      .map { column =>
        new StructField(column.getColName, column.getDataType)
      }
    CarbonTableOutputFormat.setInputSchema(conf, new StructType(fieldList.asJava))
    val loadModel = CarbonTableOutputFormat.getLoadModel(conf)
    loadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    loadModel.setBadRecordsLoggerEnable(
      TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + ",false")
    loadModel.setBadRecordsAction(
      TableOptionConstant.BAD_RECORDS_ACTION.getName + ",force")
    loadModel.setIsEmptyDataBadRecord(
      DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + ",false")
    val globalSortPartitions =
      carbonTable.getTableInfo.getFactTable.getTableProperties.get("global_sort_partitions")
    if (globalSortPartitions != null) {
      loadModel.setGlobalSortPartitions(globalSortPartitions)
    }
    loadModel
  }

  def readLoadDetailDir(
                         executorService: ExecutorService,
                         detailFiles: Array[(CarbonFile, CarbonFile)],
                         detailList: util.List[LoadMetadataDetails]): Unit = {
    val startTime = System.currentTimeMillis()
    detailFiles.map { detail =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          SegmentStatusManager
            .readTableStatusFile(detail._1.getCanonicalPath)
            .map { detail =>
              detailList.add(detail)
            }
        }
      })
    }.map { future =>
      future.get()
    }
    LOGGER.info("load detail files taken " + (System.currentTimeMillis() - startTime) + "ms")
  }

  private def backupLoadDetailDir(
                                   executorService: ExecutorService,
                                   loadDetailsHistoryDir: String,
                                   detailFiles: Array[(CarbonFile, CarbonFile)]) = {
    // backup detail files
    detailFiles.map { files =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          backupDetail(files._1, loadDetailsHistoryDir)
          backupDetail(files._2, loadDetailsHistoryDir)
        }
      })
    }.map { future =>
      future.get()
    }
    LOGGER.info("finished to copy load detail files to " + loadDetailsHistoryDir)
  }

  private def deleteTempSegmentFiles(
      executorService: ExecutorService,
      segmentFilePaths: Seq[String]): Unit = {
    LOGGER.info(s"start to delete ${segmentFilePaths.length} temp segment files")
    val start = System.currentTimeMillis()
    segmentFilePaths.map { file =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          FileFactory.deleteFile(file, FileFactory.getFileType(file))
        }
      })
    }.map { future =>
      future.get()
    }
    val end = System.currentTimeMillis()
    LOGGER.info(s"finished to delete ${segmentFilePaths.length} temp segment files, " +
                s"time taken ${end - start}ms")
  }

  private def deleteLoadDetailDir(
                                   executorService: ExecutorService,
                                   detailFiles: Array[(CarbonFile, CarbonFile)]) = {
    LOGGER.info(s"start to delete ${detailFiles.length} load detail files")
    val start = System.currentTimeMillis()
    detailFiles.map { files =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          files._1.delete()
          files._2.delete()
        }
      })
    }.map { future =>
      future.get()
    }
    val end = System.currentTimeMillis()
    LOGGER.info(s"finished to delete ${detailFiles.length} load detail files, " +
                s"time taken ${end - start}ms")
  }

  private def updateTableStatus(
                                 carbonTable: CarbonTable,
                                 tablePath: String,
                                 detailList: util.List[LoadMetadataDetails]) = {
    val tablestatusLock = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
      .getTableStatusLock()
    try {
      if (tablestatusLock.lockWithRetries) {
        LOGGER.info(
          "Acquired lock for table" + carbonTable.getDatabaseName() + "."
            + carbonTable.getTableName()
            + " for table status updation during collecting segments")
        val tablestatus = CarbonTablePath.getTableStatusFilePath(tablePath)
        // read old details
        val oldDetails = SegmentStatusManager.readTableStatusFile(tablestatus)
        val oldDetailMap = oldDetails.map { detail =>
          (detail.getSegmentFile, detail)
        }.toMap
        var startSegmentId = SegmentStatusManager.createNewSegmentId(oldDetails)
        // update segment id of new details
        val newDetails =
          detailList
            .asScala
            .filter { detail =>
              !oldDetailMap.contains(detail.getSegmentFile)
            }
            .map { detail =>
              detail.setLoadName("" + startSegmentId)
              startSegmentId = startSegmentId + 1
              detail
            }
        LOGGER.info("finished to delete load detail files")
        if (newDetails.nonEmpty) {
          SegmentStatusManager.writeLoadDetailsIntoFile(tablestatus, oldDetails ++ newDetails)
        }
      } else {
        LOGGER.error(
          "Not able to acquire the lock for Table status updation during collecting segments" +
            carbonTable.getDatabaseName() + "." + carbonTable.getTableName())
      }
    } finally {
      if (tablestatusLock.unlock) {
        LOGGER.info("Table unlocked successfully after table status updation during collecting" +
          " segments" + carbonTable.getDatabaseName + "." + carbonTable.getTableName)
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + carbonTable.getDatabaseName + "." +
          carbonTable.getTableName + " after table status updating during collect segments")
      }
    }
  }

  private def backupDetail(file: CarbonFile, backupFolder: String): Unit = {
    val filePath = file.getCanonicalPath
    val backupPath = backupFolder + "/" + file.getName
    val in = FileFactory.getDataInputStream(filePath, FileFactory.getFileType(filePath))
    val out = FileFactory.getDataOutputStream(backupPath, FileFactory.getFileType(backupPath))
    try {
      IOUtils.copyBytes(in, out, 4096)
    } finally {
      try {
        CarbonUtil.closeStream(in)
        CarbonUtil.closeStream(out)
      } catch {
        case exception: IOException =>
          LOGGER.error(exception.getMessage, exception)
      }
    }
  }

  def listLoadDetails(
                       loadDetailsDir: String,
                       hadoopConf: Configuration
                     ): Array[(CarbonFile, CarbonFile)] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir, hadoopConf)
    if (dir.exists()) {
      val allFiles = dir.listFiles()
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.LOAD_DETAILS_SUBFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap
      allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.LOAD_DETAILS_SUBFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.map { file =>
        (file, successFiles.get(file.getName).get)
      }
    } else {
      Array.empty
    }
  }

  private def lockPath(table: CarbonTable): String =
    CarbonTablePath.getLoadDetailsDir(table.getTablePath) +
    CarbonCommonConstants.FILE_SEPARATOR + "lock"

  private def acquiredLock(table: CarbonTable, conf: Configuration): Unit = {
    val lock = lockPath(table)
    if (FileFactory.isFileExist(lock)) {
      // means collect segment command is running
      // we do not allow concurrent run of collect segment command
      throw new ConcurrentOperationException(table.getDatabaseName, table.getTableName,
        "collect segment", "collect segment")
    }

    try {
      FileFactory.mkdirs(lock, conf)
    } catch {
      case e: Throwable =>
        LOGGER.error("not able to create lock for collect segment", e)
        throw e
    }
  }

  private def releaseLock(table: CarbonTable, conf: Configuration): Unit = {
    val lock = lockPath(table)
    try {
      FileFactory.deleteFile(lock, FileFactory.getFileType(lock))
    } catch {
      case e: Throwable =>
        LOGGER.error("not able to delete lock for collect segment", e)
        throw e
    }
  }

  override protected def opName: String = "COLLECT SEGMENTS"
}
