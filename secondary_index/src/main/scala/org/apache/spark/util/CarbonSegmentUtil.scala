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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormatExtended
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.rdd.CarbonScanRDD


object CarbonSegmentUtil {

  /**
   * Return's the valid segments for the query based on the filter condition
   * present in carbonScanRdd.
   *
   * @param carbonScanRdd
   * @return Array of valid segments
   */
  def getFilteredSegments(carbonScanRdd: CarbonScanRDD[InternalRow]): Array[String] = {
    val LOGGER = LogServiceFactory.getLogService(CarbonSegmentUtil.getClass.getName)
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val format = carbonScanRdd.prepareInputFormatForDriver(job.getConfiguration)
    val startTime = System.currentTimeMillis()
    val segmentsToAccess: Array[Segment] = CarbonTableInputFormatExtended
      .getFilteredSegments(job, format).asScala.toArray
    LOGGER.info(
      "Time taken for getting the Filtered segments"
      + (System.currentTimeMillis - startTime) + " ,Total segments: " + segmentsToAccess.length)
    val segmentIdtoAccess = new Array[String](segmentsToAccess.length)
    for (i <- 0 until segmentsToAccess.length) {
      segmentIdtoAccess(i) = segmentsToAccess(i).getSegmentNo
    }
    segmentIdtoAccess
  }

  /**
   * Return's an array of valid segment numbers based on the filter condition provided in the sql
   *
   * @param query
   * @param sparkSession
   * @return Array of valid segments
   * @throws UnsupportedOperationException because Get Filter Segments API supports if and only
   *                                       if only one carbon main table is present in query.
   */
  def getFilteredSegments(query: String, sparkSession: SparkSession): Array[String] = {
    val dataFrame = sparkSession.sql(s"$query")
    def isMainTableRDD(rdd: CarbonScanRDD[InternalRow]): Boolean = {
      val tableInfo = rdd.getTableInfo
      val carbonTable = CarbonTable.buildFromTableInfo(tableInfo)
      if (!CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
        return true
      }
      false
    }
    val scanRDD =
      try {
        val collectRDD: Seq[CarbonScanRDD[InternalRow]] = dataFrame.queryExecution.sparkPlan
          .collect {
            case scan: CarbonDataSourceScan
              if scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] &&
                 isMainTableRDD(scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]) =>
              scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
            case scan: RowDataSourceScanExec
              if scan.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] &&
                 isMainTableRDD(scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]) =>
              scan.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
          }
        // If collectRDD length is 0 or greater than 1 then throw exception
        if (1 != collectRDD.length) {
          sys.error("Get Filter Segments API supports if and only if only " +
                    "one carbon main table is present in query.")
        }
        collectRDD
      } catch {
        case ex: Exception =>
          throw new UnsupportedOperationException(ex.getMessage)
      }
    getFilteredSegments(scanRDD.head)
  }

  /**
   * Identifies all segments which can be merged with compaction type - MAJOR.
   *
   * @param sparkSession
   * @param tableName
   * @param dbName
   * @return list of LoadMetadataDetails
   * @throws UnsupportedOperationException if Segment length is 0 or 1
   */
  def identifySegmentsToBeMerged(sparkSession: SparkSession,
      tableName: String,
      dbName: String): util.List[LoadMetadataDetails] = {
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      CompactionType.MAJOR)
    if (segments.toList.isEmpty || segments.length.equals(1)) {
      throw new UnsupportedOperationException(
        "Compaction requires atleast 2 segments.But the input list size is " + segments.length)
    }
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        segments.toList.asJava,
        CompactionType.MAJOR,
        new util.ArrayList[String]())
  }

  /**
   * Identifies all segments which can be merged for compaction type - CUSTOM.
   *
   * @param sparkSession
   * @param tableName
   * @param dbName
   * @param customSegments
   * @return list of LoadMetadataDetails
   * @throws UnsupportedOperationException   if customSegments is null or empty
   * @throws MalformedCarbonCommandException if segment does not exist or is not valid
   */
  def identifySegmentsToBeMergedCustom(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      customSegments: util.List[String]): util.List[LoadMetadataDetails] = {
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      CompactionType.CUSTOM)
    if (customSegments.equals(null) || customSegments.isEmpty) {
      throw new UnsupportedOperationException("Custom Segments cannot be null or empty")
    }
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        segments.toList.asJava,
        CompactionType.CUSTOM,
        customSegments)
  }

  /**
   * Returns the Merged Load Name for given list of segments
   *
   * @param list
   * @return Merged Load Name
   * @throws UnsupportedOperationException if list of segments is less than 1
   */
  def getMergedLoadName(list: util.List[LoadMetadataDetails]): String = {
    if (list.size() > 1) {
      val sortedSegments: java.util.List[LoadMetadataDetails] =
        new java.util.ArrayList[LoadMetadataDetails](list)
      CarbonDataMergerUtil.sortSegments(sortedSegments)
      CarbonDataMergerUtil.getMergedLoadName(sortedSegments)
    } else {
      throw new UnsupportedOperationException(
        "Compaction requires atleast 2 segments to be merged.But the input list size is " +
        list.size())
    }
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
