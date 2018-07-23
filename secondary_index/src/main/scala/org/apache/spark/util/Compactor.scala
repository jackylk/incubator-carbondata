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
package org.apache.spark.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.command.{SecondaryIndex, SecondaryIndexModel}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.rdd.SecondaryIndexCreator
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil
import org.apache.carbondata.spark.util.CommonUtil

/**
 *
 */
object Compactor {

  /**
   * This method will create secondary index for all the index tables after compaction is completed
   *
   * @param sqlContext
   * @param carbonLoadModel
   * @param validSegments
   * @param segmentIdToLoadStartTimeMapping
   */
  def createSecondaryIndexAfterCompaction(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      validSegments: scala.List[String],
      loadsToMerge: Array[String],
      segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
      forceAccessSegment: Boolean = false): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // get list from carbonTable.getIndexes method
    if (null == CarbonInternalScalaUtil.getIndexesMap(carbonMainTable)) {
      throw new Exception("Secondary index load failed")
    }
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
        validSegments,
        segmentIdToLoadStartTimeMapping)
      try {
        val segmentToSegmentTimestampMap: java.util.Map[String, String] = new java.util
        .HashMap[String, String]()
        val indexCarbonTable = SecondaryIndexCreator
          .createSecondaryIndex(secondaryIndexModel,
            segmentToSegmentTimestampMap,
            forceAccessSegment)
        CarbonInternalLoaderUtil.updateLoadMetadataWithMergeStatus(
          indexCarbonTable,
          loadsToMerge,
          validSegments.head,
          carbonLoadModel,
          segmentToSegmentTimestampMap,
          segmentIdToLoadStartTimeMapping.get(validSegments.head).get)
        // merge index files
        CommonUtil.mergeIndexFiles(sqlContext.sparkContext,
          secondaryIndexModel.validSegments,
          segmentToSegmentTimestampMap,
          indexCarbonTable.getTablePath,
          indexCarbonTable, false)
      } catch {
        case ex: Exception =>
          LOGGER.error(ex, s"Compaction failed for SI table ${secondaryIndex.indexTableName}")
          throw ex
      }
    }
  }
}
