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

import org.junit.Test
import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.merger.CompactionType
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * This test class will test the functionality of APIs
 * present in CarbonSegmentUtil class
 */
class TestCarbonSegmentUtil extends QueryTest {

  val tableName: String = "test_table"
  val databaseName: String = "default"

  @Test
  // Test get Filtered Segments using the carbonScanRDD
  def test_getFilteredSegments() {
    createTable(tableName)
    val dataFrame = sql(s"select * from $tableName")
    val scanRdd = dataFrame.queryExecution.sparkPlan.collect {
      case b: CarbonDataSourceScan if b.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] => b.rdd
        .asInstanceOf[CarbonScanRDD[InternalRow]]
    }.head
    val expected = CarbonSegmentUtil.getFilteredSegments(scanRdd)
    assert(expected.length == 4)
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the Data Frame
  def test_getFilteredSegmentsUsingDataFrame() {
    createTable(tableName)
    val dataFrame = sql(s"select * from $tableName")
    val expected = CarbonSegmentUtil.getFilteredSegments(dataFrame)
    assert(expected.length == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Major Compaction
  def test_identifySegmentsToBeMerged_Major() {
    createTable(tableName)
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName,
        CompactionType.MAJOR)
    assert(expected.size() == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Minor Compaction
  def test_identifySegmentsToBeMerged_Minor() {
    createTable(tableName)
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName,
        CompactionType.MINOR)
    assert(expected.size() == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Invalid Compaction type
  def test_identifySegmentsToBeMerged_Invalid() {
    createTable(tableName)
    val exception = intercept[Exception] {
      CarbonSegmentUtil
        .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
          tableName,
          databaseName,
          CompactionType.IUD_DELETE_DELTA)
    }
    exception.getMessage.contains("Unsupported Compaction type")
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Custom Compaction type
  def test_identifySegmentsToBeMerged_Custom() {
    createTable(tableName)
    val exception = intercept[Exception] {
      CarbonSegmentUtil
        .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
          tableName,
          databaseName,
          CompactionType.CUSTOM)
    }
    exception.getMessage.contains("Unsupported Compaction Type. Please Use " +
                                  "identifySegmentsToBeMergedCustom API for Custom Compaction type")
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Custom Compaction type
  def test_identifySegmentsToBeMergedCustom() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMergedCustom(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName,
        CompactionType.CUSTOM,
        loadMetadataDetails.toList.asJava)
    assert(expected.size() == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with MAJOR type
  def test_identifySegmentsToBeMergedCustom_MAJOR() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val exception = intercept[Exception] {
      CarbonSegmentUtil
        .identifySegmentsToBeMergedCustom(Spark2TestQueryExecutor.spark,
          tableName,
          databaseName,
          CompactionType.MAJOR,
          loadMetadataDetails.toList.asJava)
    }
    exception.getMessage.contains("Unsupported Compaction Type. Please Use " +
                                  "identifySegmentsToBeMerged API for MINOR/MAJOR Compaction")
    dropTables(tableName)
  }

  @Test
  // Verify merged load name
  def test_getMergedLoadName() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val expected = CarbonSegmentUtil
      .getMergedLoadName(new java.util.ArrayList[LoadMetadataDetails](loadMetadataDetails.toList
        .asJava))
    assert(expected.equalsIgnoreCase("Segment_0.1"))
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with compaction threshold
  def test_identifySegmentsToBeMerged_Compaction_Threshold() {
    createTable(tableName)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName,
        CompactionType.MINOR)
    assert(expected.size() == 2)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the query with set segments
  def test_getFilteredSegments_set_segments() {
    createTable(tableName)
    val dataFrame = sql(s"select * from $tableName")
    val expected = CarbonSegmentUtil.getFilteredSegments(dataFrame)
    assert(expected.length == 4)
    sql(s"set carbon.input.segments.$databaseName.$tableName=0")
    val dataFrame_with_set_seg = sql(s"select count(*) from $tableName where c1='c1v1'")
    assert(dataFrame_with_set_seg.collect().length == 1)
    sql("reset")
    dropTables(tableName)
  }

  def createTable(tableName: String) {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 int, c3 string) STORED BY 'carbondata'")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', 2, 'c3v2'")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $tableName SELECT 'c1v2', 2, 'c3v2'")
  }

  def dropTables(tableName: String) {
    sql(s"drop table if exists $tableName")
  }

}
