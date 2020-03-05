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

package org.apache.carbondata.spark.testsuite.segment

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

/**
 * Test Class for SHOW SEGMENTS command
 */
class ShowSegmentTestCase extends QueryTest with BeforeAndAfterAll {

  test("verify success case") {
    sql("drop table if exists source")
    sql(
      """
        |create table source (age int)
        |STORED AS carbondata
        |partitioned by (name string, class string)
        |TBLPROPERTIES('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='2,2')
        |""".stripMargin)
    sql("insert into source select 1, 'abc1', 'classA'")
    sql("insert into source select 2, 'abc2', 'classB'")
    sql("insert into source select 3, 'abc3', 'classA'")
    sql("insert into source select 4, 'abc4', 'classB'")
    sql("insert into source select 5, 'abc5', 'classA'")
    sql("insert into source select 6, 'abc6', 'classC'")
    sql("show segments on source").show(false)

    val df = sql(s"""show segments on source""").collect()
    // validating headers
    val header = df(0).schema
    assert(header(0).name.equalsIgnoreCase("ID"))
    assert(header(1).name.equalsIgnoreCase("Status"))
    assert(header(2).name.equalsIgnoreCase("Load Start Time"))
    assert(header(3).name.equalsIgnoreCase("Spent"))
    assert(header(4).name.equalsIgnoreCase("Partition"))
    assert(header(5).name.equalsIgnoreCase("Data Size"))
    assert(header(6).name.equalsIgnoreCase("Index Size"))
    val col = df
      .map(row => Row(row.getString(0), row.getString(1)))
      .filter(_.getString(1).equals("Success"))
      .toSeq
    assert(col.equals(Seq(Row("4.1", "Success"), Row("0.2", "Success"))))

    sql(
      """
        | show segments on source as
        | select ID, status, DataSize from source_segments where status = 'Success' order by dataSize
        |""".stripMargin).show(false)

    val rows = sql(
      """
        | show segments on source as
        | select id, status, dataSize from source_segments where status = 'Success' order by dataSize
        |""".stripMargin).collect()

    assert(rows(0).get(0).equals("id  | status  | dataSize"))
    assert(rows(1).get(0).equals("4.1 | Success | 1762    "))
    assert(rows(2).get(0).equals("0.2 | Success | 3524    "))

    sql(s"""drop table source""").collect
  }

  test("Show Segments on empty table") {
    sql(s"""drop TABLE if exists source""").collect
    sql(s"""CREATE TABLE source (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES('table_blocksize'='1')""").collect
    checkAnswer(sql("show segments on source"), Seq.empty)
    val result = sql("show segments on source as select * from source_segments").collect()
    assertResult(0)(result.length)
  }

  test("can not show segments on exist table") {
    sql("drop TABLE if exists source").collect
    sql(
      """
        |create table source (age int, name string, class string)
        |STORED AS carbondata
        |""".stripMargin)
    sql("insert into source select 1, 'abc1', 'classA'")
    sql("drop table if exists source_segments")
    sql("create table source_segments (age int)")
    val ex = intercept[MalformedCarbonCommandException](sql("show segments on source as select * from source_segments"))
    assert(ex.getMessage.contains("source_segments already exists"))
    sql("drop TABLE if exists source")
    sql("drop table if exists source_segments")
  }

  test("can not show segments by wrong query") {
    sql("drop TABLE if exists source").collect
    sql(
      """
        |create table source (age int, name string, class string)
        |STORED AS carbondata
        |""".stripMargin)
    sql("insert into source select 1, 'abc1', 'classA'")
    val ex = intercept[MalformedCarbonCommandException](sql("show segments on source as select dsjk from source_segments"))
    assert(ex.getMessage.contains("failed to run query"))
    sql("drop TABLE if exists source")
  }

  //Show Segments failing if table name not in same case
  test("DataLoadManagement001_830") {
    sql(s"""drop TABLE if exists Case_ShowSegment_196""").collect
    sql(s"""CREATE TABLE Case_ShowSegment_196 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES('table_blocksize'='1')""").collect
    val df = sql(s"""show segments on default.CASE_ShowSegment_196""").collect()
    val col = df.map {
      row => Row(row.getString(0), row.getString(1), row.getString(4))
    }.toSeq
    assert(col.equals(Seq()))
    sql(s"""drop table Case_ShowSegment_196""").collect
  }

  test("separate visible and invisible segments info into two files") {
    val tableName = "test_tablestatus_history"
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName} (name String, age int) STORED AS carbondata "
        + "TBLPROPERTIES('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='2,2')")
    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), tableName)(sqlContext.sparkSession)
    sql(s"insert into ${tableName} select 'abc1',1")
    sql(s"insert into ${tableName} select 'abc2',2")
    sql(s"insert into ${tableName} select 'abc3',3")
    sql(s"insert into ${tableName} select 'abc4',4")
    sql(s"insert into ${tableName} select 'abc5',5")
    sql(s"insert into ${tableName} select 'abc6',6")
    assert(sql(s"show segments on ${tableName} as select * from ${tableName}_segments").collect().length == 11)
    var detail = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    var historyDetail = SegmentStatusManager.readLoadHistoryMetadata(carbonTable.getMetadataPath)
    assert(detail.length == 10)
    assert(historyDetail.length == 0)
    sql(s"clean files for table ${tableName}")
    assert(sql(s"show segments on ${tableName}").collect().length == 2)
    detail = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    historyDetail = SegmentStatusManager.readLoadHistoryMetadata(carbonTable.getMetadataPath)
    assert(detail.length == 4)
    assert(historyDetail.length == 6)
    dropTable(tableName)
  }

  test("show history segments") {
    val tableName = "test_tablestatus_history"
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName} (name String, age int) STORED AS carbondata "
        + "TBLPROPERTIES('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='2,2')")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", tableName)
    sql(s"insert into ${tableName} select 'abc1',1")
    sql(s"insert into ${tableName} select 'abc2',2")
    sql(s"insert into ${tableName} select 'abc3',3")
    sql(s"insert into ${tableName} select 'abc4',4")
    sql(s"insert into ${tableName} select 'abc5',5")
    sql(s"insert into ${tableName} select 'abc6',6")
    assert(sql(s"show segments on ${tableName} as select * from ${tableName}_segments").collect().length == 11)
    assert(sql(s"show history segments on ${tableName} as select * from ${tableName}_segments").collect().length == 11)
    sql(s"clean files for table ${tableName}")
    assert(sql(s"show segments on ${tableName} as select * from ${tableName}_segments").collect().length == 3)
    sql(s"show history segments on ${tableName} as select * from ${tableName}_segments").show(false)
    val segmentsHistoryList = sql(s"show history segments on ${tableName} as select * from ${tableName}_segments").collect()
    assert(segmentsHistoryList.length == 11)
    assert(segmentsHistoryList(1).getString(0).contains("0   | Compacted"))
    assert(segmentsHistoryList(1).getString(0).contains("| 0.1        | columnar_v3 | NA   |"))
    assert(segmentsHistoryList(2).getString(0).contains("0.2 | Success"))
    assert(segmentsHistoryList(3).getString(0).contains("5   | Compacted"))
    assert(segmentsHistoryList(3).getString(0).contains("| 4.1"))
    assert(segmentsHistoryList(4).getString(0).contains("4.1 | Success"))
    assert(segmentsHistoryList(5).getString(0).contains("1   | Compacted"))
    assert(segmentsHistoryList(5).getString(0).contains("| 0.1"))
    assert(segmentsHistoryList(8).getString(0).contains("3   | Compacted"))
    assert(segmentsHistoryList(9).getString(0).contains("2.1 | Compacted"))
    assert(segmentsHistoryList(10).getString(0).contains("4   | Compacted"))
    assert(sql(s"show history segments on ${tableName} as select * from ${tableName}_segments limit 3").collect().length == 4)
    dropTable(tableName)
  }
}