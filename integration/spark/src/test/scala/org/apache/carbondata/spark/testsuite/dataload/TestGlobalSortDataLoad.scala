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

package org.apache.carbondata.spark.testsuite.dataload

import scala.collection.JavaConverters._
import java.io.{File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.test.TestQueryExecutor.projectPath
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestGlobalSortDataLoad extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  var filePath: String = s"$resourcesPath/globalsort"

  override def beforeEach {
    resetConf()

    sql("DROP TABLE IF EXISTS carbon_globalsort")
    sql(
      """
        | CREATE TABLE carbon_globalsort(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'name, city')
      """.stripMargin)
  }

  override def afterEach {
    resetConf()

    sql("DROP TABLE IF EXISTS carbon_globalsort")
  }

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon_localsort_once")
    sql(
      """
        | CREATE TABLE carbon_localsort_once(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort_once")
  }

  override def afterAll {
    defaultConfig()
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
      CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
    sql("DROP TABLE IF EXISTS carbon_localsort_once")
    sql("DROP TABLE IF EXISTS carbon_localsort_twice")
    sql("DROP TABLE IF EXISTS carbon_localsort_triple")
    sql("DROP TABLE IF EXISTS carbon_localsort_delete")
    sql("DROP TABLE IF EXISTS carbon_localsort_update")
    sql("DROP TABLE IF EXISTS carbon_localsort_difftypes")
    sql("DROP TABLE IF EXISTS carbon_globalsort")
    sql("DROP TABLE IF EXISTS carbon_globalsort1")
    sql("DROP TABLE IF EXISTS carbon_globalsort2")
    sql("DROP TABLE IF EXISTS carbon_globalsort_partitioned")
    sql("DROP TABLE IF EXISTS carbon_globalsort_difftypes")
    sql("DROP TABLE IF EXISTS carbon_globalsort_minor")
    sql("DROP TABLE IF EXISTS carbon_globalsort_major")
    sql("DROP TABLE IF EXISTS carbon_globalsort_custom")
  }

  // ----------------------------------- Compare Result -----------------------------------
  test("Make sure the result is right and sorted in global level") {
    sql("DROP TABLE IF EXISTS carbon_globalsort1")
    sql(
      """
        | CREATE TABLE carbon_globalsort1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'name, city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort1 " +
      "OPTIONS('GLOBAL_SORT_PARTITIONS'='1')")

    assert(getIndexFileCount("carbon_globalsort1") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort1"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort1"),
      sql("SELECT * FROM carbon_localsort_once ORDER BY name"))
  }

  // ----------------------------------- Bad Record -----------------------------------
  test("Test GLOBAL_SORT with BAD_RECORDS_ACTION = 'FAIL'") {
    intercept[Exception] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
        "OPTIONS('BAD_RECORDS_ACTION'='FAIL')")
    }
  }

  test("Test GLOBAL_SORT with BAD_RECORDS_ACTION = 'REDIRECT'") {
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
      "OPTIONS('BAD_RECORDS_ACTION'='REDIRECT')")

    assert(getIndexFileCount("carbon_globalsort") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort"), Seq(Row(11)))
  }

  // ----------------------------------- Configuration Validity -----------------------------------

  test("Number of partitions should be greater than 0") {
    intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='0')")
    }

    intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='a')")
    }
  }

  // ----------------------------------- Compaction -----------------------------------
  test("compaction major: timestamp and long data type confliction")
  {
    sql("drop table if exists compactionTable")
    sql("create table compactionTable (DOJ timestamp, DOB date) STORED AS carbondata")
    sql("alter table compactionTable set tblproperties('sort_columns'='doj, dob', 'sort_scope'='global_sort')")
    sql("INSERT INTO compactionTable select '2017-10-12 21:22:23', '1997-10-10'")
    sql("INSERT INTO compactionTable select '2018-11-12 20:22:23', '1997-10-10'")
    sql("alter table compactionTable compact 'major'")
    val showSegments = sql("show segments for table compactiontable").collect()
    showSegments.find(_.get(0).toString.equals("0.1")) match {
      case Some(row) => assert(row.get(1).toString.equalsIgnoreCase("Success"))
    }
    showSegments.find(_.get(0).toString.equals("1")) match {
      case Some(row) => assert(row.get(1).toString.equalsIgnoreCase("Compacted"))
    }
    showSegments.find(_.get(0).toString.equals("0")) match {
      case Some(row) => assert(row.get(1).toString.equalsIgnoreCase("Compacted"))
    }
  }
  test("Compaction GLOBAL_SORT * 2") {
    sql("DROP TABLE IF EXISTS carbon_localsort_twice")
    sql(
      """
        | CREATE TABLE carbon_localsort_twice(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'name, city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort_twice")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort_twice")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql("ALTER TABLE carbon_globalsort COMPACT 'MAJOR'")

    assert(getIndexFileCount("carbon_globalsort") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort"), Seq(Row(24)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort ORDER BY name, id"),
      sql("SELECT * FROM carbon_localsort_twice ORDER BY name, id"))
  }

  test("Compaction GLOBAL_SORT: minor") {
    sql("DROP TABLE IF EXISTS carbon_globalsort_minor")
    sql(
      """
        | CREATE TABLE carbon_globalsort_minor(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES(
        | 'SORT_SCOPE'='GLOBAL_SORT',
        | 'sort_columns' = 'name, city',
        | 'AUTO_LOAD_MERGE'='false',
        | 'COMPACTION_LEVEL_THRESHOLD'='3,0', 'GLOBAL_SORT_PARTITIONS'='3')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_minor")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_minor")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_minor")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_minor")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    assertResult(4)(sql("show segments for table carbon_globalsort_minor").count())
    sql("ALTER TABLE carbon_globalsort_minor COMPACT 'MINOR'")
    assertResult(5)(sql("show segments for table carbon_globalsort_minor").count())
    assertResult(3)(
      sql("show segments for table carbon_globalsort_minor").rdd.filter(_.get(1).equals("Compacted")).count())
    assert(getIndexFileCount("carbon_globalsort_minor", "0.1") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort_minor"), Seq(Row(48)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort_minor ORDER BY name, id"),
      sql("SELECT * FROM carbon_globalsort ORDER BY name, id"))
  }

  test("Compaction GLOBAL_SORT: major") {
    sql("DROP TABLE IF EXISTS carbon_globalsort_major")
    sql(
      """
        | CREATE TABLE carbon_globalsort_major(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES(
        | 'SORT_SCOPE'='GLOBAL_SORT',
        | 'sort_columns' = 'name, city',
        | 'AUTO_LOAD_MERGE'='false',
        | 'MAJOR_COMPACTION_SIZE'='1024', 'GLOBAL_SORT_PARTITIONS'='4')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_major")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_major")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_major")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_major")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    assertResult(4)(sql("show segments for table carbon_globalsort_major").count())
    sql("ALTER TABLE carbon_globalsort_major COMPACT 'major'")
    assertResult(5)(sql("show segments for table carbon_globalsort_major").count())
    assertResult(4)(
      sql("show segments for table carbon_globalsort_major").rdd.filter(_.get(1).equals("Compacted")).count())
    assert(getIndexFileCount("carbon_globalsort_major", "0.1") === 4)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort_major"), Seq(Row(48)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort_major ORDER BY name, id"),
      sql("SELECT * FROM carbon_globalsort ORDER BY name, id"))
  }

  test("Compaction GLOBAL_SORT: custom") {
    sql("DROP TABLE IF EXISTS carbon_globalsort_custom")
    sql(
      """
        | CREATE TABLE carbon_globalsort_custom(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES(
        | 'SORT_SCOPE'='GLOBAL_SORT',
        | 'sort_columns' = 'name, city',
        | 'AUTO_LOAD_MERGE'='false', 'GLOBAL_SORT_PARTITIONS'='3')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_custom")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_custom")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_custom")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort_custom")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    assertResult(4)(sql("show segments for table carbon_globalsort_custom").count())
    sql("ALTER TABLE carbon_globalsort_custom COMPACT 'custom' WHERE SEGMENT.ID IN (0,1,2)")
    assertResult(5)(sql("show segments for table carbon_globalsort_custom").count())
    assertResult(3)(
      sql("show segments for table carbon_globalsort_custom").rdd.filter(_.get(1).equals("Compacted")).count())
    assert(getIndexFileCount("carbon_globalsort_custom", "0.1") === 3)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort_custom"), Seq(Row(48)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort_custom ORDER BY name, id"),
      sql("SELECT * FROM carbon_globalsort_custom ORDER BY name, id"))
  }

  // ----------------------------------- Check Configurations -----------------------------------
  // Waiting for merge SET feature[CARBONDATA-1065]
  ignore("DDL > SET") {
    sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = LOCAL_SORT")
    sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = 5")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
      "OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")

    assert(getIndexFileCount("carbon_globalsort") === 2)
  }

  test("DDL > carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "LOCAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "5")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='2')")

    assert(getIndexFileCount("carbon_globalsort") === 2)
  }

  // Waiting for merge SET feature[CARBONDATA-1065]
  ignore("SET > carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "LOCAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "5")
    sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = GLOBAL_SORT")
    sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = 2")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")

    assert(getIndexFileCount("carbon_globalsort") === 2)
  }

  test("carbon.properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "GLOBAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "2")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")

    assert(getIndexFileCount("carbon_globalsort") === 2)
  }

  // ----------------------------------- IUD -----------------------------------
  test("LOAD with DELETE") {
    sql("DROP TABLE IF EXISTS carbon_localsort_delete")
    sql(
      """
        | CREATE TABLE carbon_localsort_delete(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'name, city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort_delete")
    sql("DELETE FROM carbon_localsort_delete WHERE id = 1").show

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql("DELETE FROM carbon_globalsort WHERE id = 1").show

    assert(getIndexFileCount("carbon_globalsort") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort"), Seq(Row(11)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort ORDER BY name, id"),
      sql("SELECT * FROM carbon_localsort_delete ORDER BY name, id"))
  }

  test("LOAD with UPDATE") {
    sql("DROP TABLE IF EXISTS carbon_localsort_update")
    sql(
      """
        | CREATE TABLE carbon_localsort_update(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'name, city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_localsort_update")

    sql("UPDATE carbon_localsort_update SET (name) = ('bb') WHERE id = 2").show
    sql("select * from carbon_localsort_update").show()
    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_globalsort")
    sql("select * from carbon_globalsort").show()
    sql("UPDATE carbon_globalsort SET (name) = ('bb') WHERE id = 2").show
    sql("select * from carbon_globalsort").show()
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort"), Seq(Row(12)))
    checkAnswer(sql("SELECT name FROM carbon_globalsort WHERE id = 2"), Seq(Row("bb")))
    checkAnswer(sql("SELECT * FROM carbon_globalsort ORDER BY name, id"),
      sql("SELECT * FROM carbon_localsort_update ORDER BY name, id"))
  }

  test("LOAD with small files") {
    val inputPath = new File("target/small_files").getCanonicalPath
    val folder = new File(inputPath)
    if (folder.exists()) {
      FileUtils.deleteDirectory(folder)
    }
    folder.mkdir()
    for (i <- 0 to 100) {
      val file = s"$folder/file$i.csv"
      val writer = new FileWriter(file)
      writer.write("id,name,city,age\n")
      writer.write(s"$i,name_$i,city_$i,${ i % 100 }")
      writer.close()
    }
    sql(s"LOAD DATA LOCAL INPATH '$inputPath' INTO TABLE carbon_globalsort")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "carbon_globalsort")
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    if (FileFactory.isFileExist(segmentDir)) {
      assertResult(Math.max(4, defaultParallelism) + 1)(new File(segmentDir).listFiles().length)
    } else {
      val segment = Segment.getSegment("0", carbonTable.getTablePath)
      val store = new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName)
      store.readIndexFiles(new Configuration(false))
      val size = store.getIndexFilesMap.asScala.map(f => f._2.size()).sum
      assertResult(Math.max(4, defaultParallelism) + 1)(size + store.getIndexFilesMap.size())
    }
  }

  test("Query with small files") {
    try {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_MERGE_FILES)
      for (i <- 0 until 10) {
        sql(s"insert into carbon_globalsort select $i, 'name_$i', 'city_$i', ${ i % 100 }")
      }
      val df = sql("select * from carbon_globalsort")
      val scanRdd = df.queryExecution.sparkPlan.collect {
        case b: CarbonDataSourceScan if b.rdd.isInstanceOf[CarbonScanRDD[InternalRow]] =>
          b.rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
      }.head
      assertResult(defaultParallelism)(scanRdd.getPartitions.length)
      assertResult(10)(df.count)
    } finally {
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
    }
  }

  // ----------------------------------- INSERT INTO -----------------------------------
  test("INSERT INTO") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "GLOBAL_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS, "2")
    sql(s"INSERT INTO TABLE carbon_globalsort SELECT * FROM carbon_localsort_once")

    assert(getIndexFileCount("carbon_globalsort") === 2)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_globalsort"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_globalsort ORDER BY name, id"),
      sql("SELECT * FROM carbon_localsort_once ORDER BY name, id"))
  }

  test("Test with different date types") {
    val path = s"$projectPath/examples/spark/src/main/resources/data.csv"

    sql("DROP TABLE IF EXISTS carbon_localsort_difftypes")
    sql(
      s"""
         | CREATE TABLE carbon_localsort_difftypes(
         | shortField smallint,
         | intField INT,
         | bigintField bigint,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='stringField')
       """.stripMargin)
//    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/mm/dd")
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path' INTO TABLE carbon_localsort_difftypes
         | OPTIONS('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField')
       """.stripMargin)

    sql("DROP TABLE IF EXISTS carbon_globalsort_difftypes")
    sql(
      s"""
         | CREATE TABLE carbon_globalsort_difftypes(
         | shortField smallint,
         | intField INT,
         | bigintField bigint,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'sort_columns' = 'stringField')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path' INTO TABLE carbon_globalsort_difftypes
         | OPTIONS(
         | 'FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField')
       """.stripMargin)

    checkAnswer(sql("SELECT * FROM carbon_globalsort_difftypes ORDER BY shortField"),
      sql("SELECT * FROM carbon_localsort_difftypes ORDER BY shortField"))
  }

  private def resetConf() {
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.LOAD_SORT_SCOPE)
      .addProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
        CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT)

    // sql(s"SET ${CarbonCommonConstants.LOAD_SORT_SCOPE} = ${CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT}")
    // sql(s"SET ${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS} = " +
    //  s"${CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT}")
  }

  private def getIndexFileCount(tableName: String, segmentNo: String = "0"): Int = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", tableName)
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentNo)
    if (FileFactory.isFileExist(segmentDir)) {
      new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir).size()
    } else {
      val segment = Segment.getSegment(segmentNo, carbonTable.getTablePath)
      new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName).getIndexCarbonFiles.size()
    }
  }
}
