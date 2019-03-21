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
package org.apache.carbondata.spark.rdd

import java.util.concurrent.Callable

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, SQLContext}
import org.apache.spark.sql.command.SecondaryIndexModel
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.SecondaryIndexCreationResultImpl
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants

/**
 * This class is aimed at creating secondary index for specified segments
 */
object SecondaryIndexCreator {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def createSecondaryIndex(secondaryIndexModel: SecondaryIndexModel,
    segmentToLoadStartTimeMap: java.util.Map[String, String],
    indexTable: CarbonTable,
    forceAccessSegment: Boolean = false,
    isCompactionCall: Boolean,
    isLoadToFailedSISegments: Boolean): CarbonTable = {
    var indexCarbonTable = indexTable
    val sc = secondaryIndexModel.sqlContext
    // get the thread pool size for secondary index creation
    val threadPoolSize = getThreadPoolSize(sc)
    LOGGER
      .info(s"Configured thread pool size for distributing segments in secondary index creation " +
            s"is $threadPoolSize")
    // create executor service to parallely run the segments
    val executorService = java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize)
    if (null == indexCarbonTable) {
      // avoid more lookupRelation to table
      val metastore = CarbonEnv.getInstance(secondaryIndexModel.sqlContext.sparkSession)
        .carbonMetaStore
      indexCarbonTable = metastore
        .lookupRelation(Some(secondaryIndexModel.carbonLoadModel.getDatabaseName),
          secondaryIndexModel.secondaryIndex.indexTableName)(secondaryIndexModel.sqlContext
          .sparkSession).asInstanceOf[CarbonRelation].carbonTable
    }

    try {
      SegmentStatusManager.deleteLoadsAndUpdateMetadata(indexCarbonTable, false, null)
      TableProcessingOperations.deletePartialLoadDataIfExist(indexCarbonTable, false)
      FileInternalUtil
        .updateTableStatus(secondaryIndexModel.validSegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.INSERT_IN_PROGRESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          new java.util
          .HashMap[String,
            String](),
          indexCarbonTable,
          sc.sparkSession)
      var execInstance = "1"
      // in case of non dynamic executor allocation, number of executors are fixed.
      if (sc.sparkContext.getConf.contains("spark.executor.instances")) {
        execInstance = sc.sparkContext.getConf.get("spark.executor.instances")
        LOGGER.info("spark.executor.instances property is set to =" + execInstance)
      }
      // in case of dynamic executor allocation, taking the max executors
      // of the dynamic allocation.
      else if (sc.sparkContext.getConf.contains("spark.dynamicAllocation.enabled")) {
        if (sc.sparkContext.getConf.get("spark.dynamicAllocation.enabled").trim
          .equalsIgnoreCase("true")) {
          execInstance = sc.sparkContext.getConf.get("spark.dynamicAllocation.maxExecutors")
          LOGGER.info("spark.dynamicAllocation.maxExecutors property is set to =" + execInstance)
        }
      }
      var futureObjectList = List[java.util.concurrent.Future[Array[(String, Boolean)]]]()
      for (eachSegment <- secondaryIndexModel.validSegments) {
        val segId = eachSegment
        futureObjectList :+= executorService.submit(new Callable[Array[(String, Boolean)]] {
          @throws(classOf[Exception])
          override def call(): Array[(String, Boolean)] = {
            ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo().getNonSerializableExtraInfo
              .put("carbonConf", SparkSQLUtil.sessionState(sc.sparkSession).newHadoopConf())
            var eachSegmentSecondaryIndexCreationStatus: Array[(String, Boolean)] = Array.empty
            CarbonLoaderUtil.checkAndCreateCarbonDataLocation(segId, indexCarbonTable)
            val carbonLoadModel = getCopyObject(secondaryIndexModel)
            carbonLoadModel
              .setFactTimeStamp(secondaryIndexModel.segmentIdToLoadStartTimeMapping(eachSegment))
            carbonLoadModel.setTablePath(secondaryIndexModel.carbonTable.getTablePath)
            val secondaryIndexCreationStatus = new CarbonSecondaryIndexRDD(sc.sparkSession,
              new SecondaryIndexCreationResultImpl,
              carbonLoadModel,
              secondaryIndexModel.secondaryIndex,
              segId, execInstance, indexCarbonTable, forceAccessSegment).collect()
            val segmentFileName =
              SegmentFileStore
                .writeSegmentFile(indexCarbonTable,
                  segId,
                  String.valueOf(carbonLoadModel.getFactTimeStamp))
            segmentToLoadStartTimeMap.put(segId, String.valueOf(carbonLoadModel.getFactTimeStamp))
            if (secondaryIndexCreationStatus.length > 0) {
              eachSegmentSecondaryIndexCreationStatus = secondaryIndexCreationStatus
            }
            eachSegmentSecondaryIndexCreationStatus
          }
        })
      }

      val segmentSecondaryIndexCreationStatus = futureObjectList.groupBy(a => a.get().head._2)
      val hasSuccessSegments = segmentSecondaryIndexCreationStatus.contains("true".toBoolean)
      val hasFailedSegments = segmentSecondaryIndexCreationStatus.contains("false".toBoolean)
      var successSISegments: List[String] = List()
      var failedSISegments: List[String] = List()
      if (hasSuccessSegments) {
        successSISegments =
          segmentSecondaryIndexCreationStatus("true".toBoolean).collect {
            case segments: java.util.concurrent.Future[Array[(String, Boolean)]] =>
              segments.get().head._1
          }
      }

      if (hasFailedSegments) {
        // if the call is from compaction, we need to fail the main table compaction also, and if
        // the load is called from SIloadEventListener, which is for corresponding main table
        // segment, then if SI load fails, we need to fail main table load also, so throw exception,
        // if load is called from SI creation or SILoadEventListenerForFailedSegments, no need to
        // fail, just make the segement as marked for delete, so that next load to main table will
        // take care
        if (isCompactionCall || !isLoadToFailedSISegments) {
          throw new Exception("Secondary index creation failed")
        } else {
          failedSISegments =
            segmentSecondaryIndexCreationStatus("false".toBoolean).collect {
              case segments: java.util.concurrent.Future[Array[(String, Boolean)]] =>
                segments.get().head._1
            }
        }
      }
      // what and all segments the load failed, only for those need make status as marked
      // for delete, remaining let them be SUCCESS
      var tableStatusUpdateForSuccess = false
      var tableStatusUpdateForFailure = false

      if (successSISegments.nonEmpty && !isCompactionCall) {
        tableStatusUpdateForSuccess = FileInternalUtil.updateTableStatus(
          successSISegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.SUCCESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToLoadStartTimeMap,
          indexCarbonTable,
          secondaryIndexModel.sqlContext.sparkSession)
      }

      // update the status of all the segments to marked for delete if data load fails, so that
      // next load which is triggered for SI table in post event of main table data load clears
      // all the segments of marked for delete and retriggers the load to same segments again in
      // that event
      if (failedSISegments.nonEmpty && !isCompactionCall) {
        tableStatusUpdateForFailure = FileInternalUtil.updateTableStatus(
          failedSISegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.MARKED_FOR_DELETE,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToLoadStartTimeMap,
          indexCarbonTable,
          secondaryIndexModel.sqlContext.sparkSession)
      }

      // merge index files for success segments in case of only load
      if (!isCompactionCall) {
        CarbonMergeFilesRDD.mergeIndexFiles(secondaryIndexModel.sqlContext.sparkSession,
          successSISegments,
          segmentToLoadStartTimeMap,
          indexCarbonTable.getTablePath,
          indexCarbonTable, mergeIndexProperty = false)
      }

      if (failedSISegments.nonEmpty) {
        LOGGER.error("Dataload to secondary index creation has failed")
      }
      indexCarbonTable
    } catch {
      case ex: Exception =>
        FileInternalUtil
          .updateTableStatus(secondaryIndexModel.validSegments,
            secondaryIndexModel.carbonLoadModel.getDatabaseName,
            secondaryIndexModel.secondaryIndex.indexTableName,
            SegmentStatus.MARKED_FOR_DELETE,
            secondaryIndexModel.segmentIdToLoadStartTimeMapping,
            new java.util
            .HashMap[String,
              String](),
            indexCarbonTable,
            sc.sparkSession)
        try {
          SegmentStatusManager
            .deleteLoadsAndUpdateMetadata(indexCarbonTable, false, null)
          TableProcessingOperations.deletePartialLoadDataIfExist(indexCarbonTable, false)
        } catch {
          case e: Exception =>
            LOGGER
              .error("Problem while cleaning up stale folder for index table " +
                        secondaryIndexModel.secondaryIndex.indexTableName, e)
        }
        LOGGER.error(ex)
        throw ex
    } finally {
      // close the executor service
      if (null != executorService) {
        executorService.shutdownNow()
      }
    }
  }

  /**
   * will return the copy object of the existing object
   *
   * @return
   */
  def getCopyObject(secondaryIndexModel: SecondaryIndexModel): CarbonLoadModel = {
    val carbonLoadModel = secondaryIndexModel.carbonLoadModel
    val copyObj = new CarbonLoadModel
    copyObj.setTableName(carbonLoadModel.getTableName)
    copyObj.setDatabaseName(carbonLoadModel.getDatabaseName)
    copyObj.setLoadMetadataDetails(carbonLoadModel.getLoadMetadataDetails)
    copyObj.setCarbonDataLoadSchema(carbonLoadModel.getCarbonDataLoadSchema)
    copyObj.setColumnCompressor(CarbonInternalScalaUtil
      .getCompressorForIndexTable(secondaryIndexModel)(secondaryIndexModel
        .sqlContext.sparkSession))
    copyObj
  }

  /**
   * This method will get the configuration for thread pool size which will decide the numbe rof
   * segments to run in parallel for secondary index creation
   *
   * @param sqlContext
   * @return
   */
  def getThreadPoolSize(sqlContext: SQLContext): Int = {
    var threadPoolSize: Int = 0
    try {
      threadPoolSize = CarbonProperties.getInstance()
        .getProperty(CarbonInternalCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS,
          CarbonInternalCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT).toInt
      if (threadPoolSize >
          CarbonInternalCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_MAX) {
        threadPoolSize = CarbonInternalCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_MAX
        LOGGER
          .info(s"Configured thread pool size for secondary index creation is greater than " +
                s"default parallelism. Therefore default value will be considered: $threadPoolSize")
      } else {
        val defaultThreadPoolSize =
          CarbonInternalCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT.toInt
        if (threadPoolSize < defaultThreadPoolSize) {
          threadPoolSize = defaultThreadPoolSize
          LOGGER
            .info(s"Configured thread pool size for secondary index creation is incorrect. " +
                  s" Therefore default value will be considered: $threadPoolSize")
        }
      }
    } catch {
      case nfe: NumberFormatException =>
        threadPoolSize = CarbonInternalCommonConstants
          .CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT.toInt
        LOGGER
          .info(s"Configured thread pool size for secondary index creation is incorrect. " +
                s" Therefore default value will be considered: $threadPoolSize")
    }
    threadPoolSize
  }
}
