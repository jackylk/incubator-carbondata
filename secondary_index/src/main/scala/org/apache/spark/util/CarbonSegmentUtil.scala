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

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{CarbonEnv, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormatExtended
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.rdd.CarbonScanRDD


object CarbonSegmentUtil {

  /**
   * This method is used to get the valid segments for the query based on the filter condition.
   *
   * @param carbonScanRdd
   * @return Array of valid segments
   */
  def getFilteredSegments(carbonScanRdd: CarbonScanRDD[InternalRow]): Array[Segment] = {
    val LOGGER = LogServiceFactory.getLogService(BroadCastSIFilterPushJoin.getClass.getName)
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val format = carbonScanRdd.prepareInputFormatForDriver(job.getConfiguration)
    val startTime = System.currentTimeMillis()
    val segmentsToAccess: Array[Segment] = CarbonTableInputFormatExtended
      .getFilteredSegments(job, format).asScala.toArray
    LOGGER
      .info("Time taken for getting the splits: " + (System.currentTimeMillis - startTime) +
            " ,Total split: " + segmentsToAccess.length)
    segmentsToAccess
  }

  /**
   * This method is used to get the valid segments for the query based on the filter condition.
   *
   * @param query
   * @return Array of valid segments
   */
  def getFilteredSegments(query: DataFrame): Array[Segment] = {
    val scanRDD = query.queryExecution.sparkPlan.collect {
      case scan: CarbonDataSourceScan if scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] =>
        scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
    }.head
    getFilteredSegments(scanRDD)
  }

  /**
   * To identify which all segments can be merged with compaction type - MINOR/MAJOR.
   *
   * @param sparkSession
   * @param tableName
   * @param dbName
   * @param compactionType
   * @return list of LoadMetadataDetails
   */
  def identifySegmentsToBeMerged(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      compactionType: CompactionType): util.List[LoadMetadataDetails] = {
    compactionType match {
      case CompactionType.MINOR | CompactionType.MAJOR => true
      case CompactionType.CUSTOM =>
        sys.error(
          "Unsupported Compaction Type. Please Use identifySegmentsToBeMergedCustom API " +
          "for Custom Compaction type")
      case _ => sys.error("Unsupported Compaction type")
    }
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      compactionType)
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        segments.toList.asJava,
        compactionType,
        new util.ArrayList[String]())
  }

  /**
   * To identify which all segments can be merged for compaction type - CUSTOM.
   *
   * @param sparkSession
   * @param tableName
   * @param dbName
   * @param compactionType
   * @return list of LoadMetadataDetails
   */
  def identifySegmentsToBeMergedCustom(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      compactionType: CompactionType,
      customSegments: util.List[LoadMetadataDetails]): util.List[LoadMetadataDetails] = {
    compactionType match {
      case CompactionType.CUSTOM => true
      case CompactionType.MINOR | CompactionType.MINOR =>
        sys.error(
          "Unsupported Compaction Type. Please Use identifySegmentsToBeMerged API " +
          "for MINOR/MAJOR Compaction")
      case _ => sys.error("Unsupported Compaction Type.")
    }
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      compactionType)
    val customSegment = customSegments.asScala.map(_.getLoadName).toList.asJava
    if (customSegment.equals(null) || customSegment.isEmpty) {
      sys.error("Custom Segments cannot be null")
    }
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        customSegments,
        compactionType,
        customSegment)
  }

  /**
   * To get the Merged Load Name
   *
   * @param list
   * @return Merged Load Name
   */
  def getMergedLoadName(list: util.ArrayList[LoadMetadataDetails]): String = {
    CarbonDataMergerUtil.getMergedLoadName(list)
  }

  private def getSegmentDetails(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      compactionType: CompactionType): (CarbonLoadModel, Long, Array[LoadMetadataDetails]) = {
    val carbonLoadModel = new CarbonLoadModel
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val carbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable)
    carbonLoadModel.setCarbonDataLoadSchema(carbonDataLoadSchema)
    val compactionSize = CarbonDataMergerUtil.getCompactionSize(compactionType, carbonLoadModel)
    val segments = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    (carbonLoadModel, compactionSize, segments)
  }

}
