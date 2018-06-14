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

package org.apache.carbondata.spark.rdd

import java.util.concurrent.Callable

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.sql.{CarbonEnv, SQLContext}
import org.apache.spark.sql.command.{SecondaryIndex, SecondaryIndexModel}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CarbonInternalCommonUtil, CarbonInternalScalaUtil}
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
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

  /**
   * This method will create secondary index for all the index tables after compaction is completed
   *
   * @param sqlContext
   * @param carbonLoadModel
   * @param loadName
   * @param mergeLoadStartTime
   */
  def createSecondaryIndexAfterCompaction(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      loadName: String,
      mergeLoadStartTime: java.lang.Long): Unit = {
    val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // get list from carbonTable.getIndexes method
    if (null == CarbonInternalScalaUtil.getIndexesMap(carbonMainTable)) {
      throw new Exception("Secondary index load failed")
    }
    val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
      scala.collection.mutable.Map((loadName, mergeLoadStartTime))
    val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
    indexTablesList.foreach { indexTableAndColumns =>
      val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
        carbonLoadModel.getTableName,
        indexTableAndColumns._2.asScala.toList,
        indexTableAndColumns._1)
      val secondaryIndexModel = SecondaryIndexModel(sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        List(loadName),
        segmentIdToLoadStartTimeMapping)
      try {
        val segmentToSegmentFileNameMap: java.util.Map[String, String] = new java.util
        .HashMap[String, String]()
        val indexCarbonTable = createSecondaryIndex(secondaryIndexModel,
          segmentToSegmentFileNameMap)
        val tableStatusUpdation = FileInternalUtil.updateTableStatus(
          secondaryIndexModel.validSegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.SUCCESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToSegmentFileNameMap,
          carbonMainTable)
        // merge index files
        CarbonInternalCommonUtil.mergeIndexFiles(sqlContext.sparkContext,
          secondaryIndexModel.validSegments,
          segmentToSegmentFileNameMap,
          indexCarbonTable.getTablePath,
          indexCarbonTable, false)
        if (!tableStatusUpdation) {
          throw new Exception("Table status updation failed while creating secondary index")
        }
      } catch {
        case ex: Exception =>
          throw ex
      }
    }
  }

  def createSecondaryIndex(secondaryIndexModel: SecondaryIndexModel,
    segmentToSegmentFileNameMap: java.util.Map[String, String],
    forceAccessSegment: Boolean = false): CarbonTable = {
    val sc = secondaryIndexModel.sqlContext
    // get the thread pool size for secondary index creation
    val threadPoolSize = getThreadPoolSize(sc)
    LOGGER
      .info(s"Configured thread pool size for distributing segments in secondary index creation " +
            s"is $threadPoolSize")
    // create executor service to parallely run the segments
    val executorService = java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize)
    val metastore = CarbonEnv.getInstance(secondaryIndexModel.sqlContext.sparkSession)
      .carbonMetastore
    val indexCarbonTable = metastore
      .lookupRelation(Some(secondaryIndexModel.carbonLoadModel.getDatabaseName),
        secondaryIndexModel.secondaryIndex.indexTableName)(secondaryIndexModel.sqlContext
        .sparkSession).asInstanceOf[CarbonRelation].carbonTable

    try {
      SegmentStatusManager.deleteLoadsAndUpdateMetadata(indexCarbonTable, false, null)
      TableProcessingOperations.deletePartialLoadDataIfExist(indexCarbonTable, false)
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
      var futureObjectList = List[java.util.concurrent.Future[Boolean]]()
      for (eachSegment <- secondaryIndexModel.validSegments) {
        val segId = eachSegment
        futureObjectList :+= executorService.submit(new Callable[Boolean] {
          @throws(classOf[Exception])
          override def call(): Boolean = {
            var eachSegmentSecondaryIndexCreationStatus = false
            CarbonLoaderUtil.checkAndCreateCarbonDataLocation(segId, indexCarbonTable)
            val carbonLoadModel = getCopyObject(secondaryIndexModel.carbonLoadModel)
            carbonLoadModel
              .setFactTimeStamp(secondaryIndexModel.segmentIdToLoadStartTimeMapping.get(eachSegment)
                .get)
            carbonLoadModel.setTablePath(secondaryIndexModel.carbonTable.getTablePath)
            val secondaryIndexCreationStatus = new CarbonSecondaryIndexRDD(sc.sparkContext,
              new SecondaryIndexCreationResultImpl,
              carbonLoadModel,
              secondaryIndexModel.secondaryIndex,
              segId, execInstance, indexCarbonTable, forceAccessSegment).collect()
            val segmentFileName =
              SegmentFileStore
                .writeSegmentFile(indexCarbonTable,
                  segId,
                  String.valueOf(carbonLoadModel.getFactTimeStamp))
            segmentToSegmentFileNameMap.put(segId, segmentFileName)
            if (secondaryIndexCreationStatus.length > 0) {
              eachSegmentSecondaryIndexCreationStatus = secondaryIndexCreationStatus.forall(_._2)
            }
            eachSegmentSecondaryIndexCreationStatus
          }
        })
      }

      // check the secondary index creation from each segment, break if any segment returns false
      var secondaryIndexCreationStatus = false
      breakable {
        futureObjectList
          .foreach { future =>
            secondaryIndexCreationStatus = future.get()
            if (!secondaryIndexCreationStatus) {
              break
            }
          }
      }

      // handle success and failure scenarios for each segment secondary index creation status
      if (!secondaryIndexCreationStatus) {
        throw new Exception("Secondary index creation failed")
      }
      indexCarbonTable
    } catch {
      case ex: Exception =>
        try {
          SegmentStatusManager
            .deleteLoadsAndUpdateMetadata(indexCarbonTable, false, null)
          TableProcessingOperations.deletePartialLoadDataIfExist(indexCarbonTable, false)
        } catch {
          case e: Exception =>
            LOGGER
              .error(e, "Problem while cleaning up stale folder for index table " +
                        secondaryIndexModel.secondaryIndex.indexTableName)
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
  def getCopyObject(carbonLoadModel: CarbonLoadModel): CarbonLoadModel = {
    val copyObj = new CarbonLoadModel
    copyObj.setTableName(carbonLoadModel.getTableName)
    copyObj.setDatabaseName(carbonLoadModel.getDatabaseName)
//    copyObj.setPartitionId(carbonLoadModel.getPartitionId)
    copyObj.setLoadMetadataDetails(carbonLoadModel.getLoadMetadataDetails)
    copyObj.setCarbonDataLoadSchema(carbonLoadModel.getCarbonDataLoadSchema)
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
