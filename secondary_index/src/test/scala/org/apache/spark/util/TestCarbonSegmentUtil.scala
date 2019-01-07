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

import org.junit.Test
import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
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
    val expected = CarbonSegmentUtil
      .getFilteredSegments(s"select * from $tableName", Spark2TestQueryExecutor.spark)
    assert(expected.length == 4)
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the Data Frame with multiple tables
  def test_getFilteredSegmentsUsingDataFrame_multiple() {
    createTable(tableName)
    createTable(tableName + 1)
    val exception = intercept[Exception] {
      CarbonSegmentUtil
        .getFilteredSegments("select * from test_table t1 join test_table1 t2 on t1.c1=t2.c1",
          Spark2TestQueryExecutor.spark)
    }
    exception.getMessage.contains("Unsupported operation as table contains multiple CarbonRDDs")
  }

  @Test
  // Test get Filtered Segments using the Data Frame with non-carbon tables
  def test_getFilteredSegmentsUsingDataFrame_non_carbon_tables() {
    sql(s"drop table if exists $tableName")
    sql(s"CREATE TABLE $tableName(c1 string, c2 int, c3 string)")
    sql(s"INSERT INTO $tableName SELECT 'c1v1', 1, 'c3v1'")
    val exception = intercept[Exception] {
      CarbonSegmentUtil
        .getFilteredSegments(s"select * from $tableName",
          Spark2TestQueryExecutor.spark)
    }
    exception.getMessage.contains("Unsupported operation for non-carbon tables")
  }

  @Test
  // Test identify segments to be merged with Major Compaction
  def test_identifySegmentsToBeMerged_Major() {
    createTable(tableName)
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMerged(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName)
    assert(expected.size() == 4)
    dropTables(tableName)
  }

  @Test
  // Test identify segments to be merged with Custom Compaction type
  def test_identifySegmentsToBeMergedCustom() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
    val customSegments = new util.ArrayList[String]()
    customSegments.add("1")
    customSegments.add("2")
    val expected = CarbonSegmentUtil
      .identifySegmentsToBeMergedCustom(Spark2TestQueryExecutor.spark,
        tableName,
        databaseName,
        customSegments
      )
    assert(expected.size() == 2)
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
      .getMergedLoadName(loadMetadataDetails.toList.asJava)
    assert(expected.equalsIgnoreCase("Segment_0.1"))
    dropTables(tableName)
  }

  @Test
  // Verify merged load name with unsorted segment lsit
  def test_getMergedLoadName_unsorted_segment_list() {
    createTable(tableName)
    val carbonTable = CarbonEnv
      .getCarbonTable(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    val segments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails]()
    val load1 = new LoadMetadataDetails()
    load1.setLoadName("1")
    load1.setLoadStartTime(System.currentTimeMillis())
    segments.add(load1)
    val load = new LoadMetadataDetails()
    load.setLoadName("0")
    load.setLoadStartTime(System.currentTimeMillis())
    segments.add(load)
    val expected = CarbonSegmentUtil
      .getMergedLoadName(segments)
    println(expected)
    assert(expected.equalsIgnoreCase("Segment_0.1"))
    dropTables(tableName)
  }

  @Test
  // Test get Filtered Segments using the query with set segments
  def test_getFilteredSegments_set_segments() {
    createTable(tableName)
    val expected = CarbonSegmentUtil
      .getFilteredSegments(s"select * from $tableName", Spark2TestQueryExecutor.spark)
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
