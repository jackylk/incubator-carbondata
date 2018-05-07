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

package org.apache.carbondata.spark.testsuite.mergeindex

import java.io.{File, PrintWriter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.core.CarbonCommonPluginConstants
import org.apache.spark.sql.Row

import scala.util.Random

class CarbonIndexFileMergeTestCase
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/file2.csv"

  override protected def beforeAll(): Unit = {
    val n = 150000
    createFile(file2, n * 4, n)
  }

  override protected def afterAll(): Unit = {
    deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
  }

  test("Verify correctness of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_indexmerge", "0") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""),
      sql("""Select count(*) from indexmerge"""))
  }

  test("Verify command of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify command of index merge without enabling property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge for compacted segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("ALTER TABLE nonindexmerge COMPACT 'segment_index'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge for compacted segments MINOR") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "4.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge for compacted segments MINOR - level 2") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "9") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "10") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "11") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "9") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "10") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "11") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge for compacted segments Auto Compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "4.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(5400000)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("Verify index index merge for compacted segments Auto Compaction - level 2") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "9") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "10") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "11") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "4") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "5") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "6") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "7") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "8") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "9") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "10") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "11") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "12") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "4.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "8.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), Seq(Row(7800000)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE, "false")
  }


  private def getIndexFileCount(tableName: String, segment: String): Int = {
    val table = CarbonMetadata.getInstance().getCarbonTable(tableName)
    val path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    val carbonFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(CarbonTablePath
        .INDEX_FILE_EXT)
    })
    if (carbonFiles != null) {
      carbonFiles.length
    } else {
      0
    }
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(fileName);
      for (i <- start until (start + line)) {
        write
          .println(i + "," + "n" + i + "," + "c" + Random.nextInt(line) + "," + Random.nextInt(80))
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  private def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }

}
