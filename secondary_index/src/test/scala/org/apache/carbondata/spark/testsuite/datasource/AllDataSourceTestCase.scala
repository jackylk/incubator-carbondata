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

package org.apache.carbondata.spark.testsuite.datasource

import java.io.File

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
  * Test Class for all data source
  *
  */
class AllDataSourceTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    dropTable
    sql(
      s"""
         | create table origin_csv(col1 int, col2 string, col3 date)
         | using csv
         | options('dateFormat'='yyyy-MM-dd', 'timestampFormat'='yyyy-MM-dd HH:mm:ss')
         | """.stripMargin)
    sql("insert into origin_csv select 1, 'aa', to_date('2019-11-11')")
    sql("insert into origin_csv select 2, 'bb', to_date('2019-11-12')")
    sql("insert into origin_csv select 3, 'cc', to_date('2019-11-13')")
  }

  def dropTable {
    dropTableByName("ds_carbon")
    dropTableByName("ds_carbondata")
    dropTableByName("hive_carbon")
    dropTableByName("hive_carbondata")

    sql(s"drop table if exists tbl_truncate")
    sql(s"drop table if exists origin_csv")
  }

  def dropTableByName(tableName: String) :Unit = {
    sql(s"drop table if exists $tableName")
    sql(s"drop table if exists ${tableName}_p")
    sql(s"drop table if exists ${tableName}_ctas")
    sql(s"drop table if exists ${tableName}_e")
    sql(s"drop table if exists ${tableName}_s")
  }

  override def afterAll: Unit = {
    dropTable
  }

  test("test carbon"){
    verifyDataSourceTable("carbon", "ds_carbon")
    verifyHiveTable("carbon", "hive_carbon")
  }

  test("test carbondata"){
    verifyDataSourceTable("carbondata", "ds_carbondata")
    verifyHiveTable("carbondata", "hive_carbondata")
  }

  test("test partition table") {
    createDataSourcePartitionTable("carbondata", "ds_carbondata_p")
    createHivePartitionTable("carbondata", "hive_carbondata_p")
  }

  test("test external table") {
    val tableName = "ds_carbondata"
    val path  = s"${warehouse}/ds_external"
    val ex = intercept[MalformedCarbonCommandException](
      sql(
        s"""
           |create table ${ tableName }_s
           | using carbondata
           | LOCATION '$path'
           | as select col1, col2 from origin_csv
           | """.stripMargin))
    assert(ex.getMessage.contains("Create external table as select is not allowed"))

    sql(s"create table ${tableName}_s using carbondata as select * from origin_csv")
    val carbonTable =
      CarbonEnv.getCarbonTable(Option("default"), s"${tableName}_s")(sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    sql(s"create table  ${tableName}_e using carbondata location '${tablePath}'")
    checkAnswer(sql(s"select count(*) from ${tableName}_e"), Seq(Row(3)))
    sql(s"drop table if exists ${tableName}_e")
    assert(!CarbonPlanHelper.isCarbonTable(
      TableIdentifier(s"${tableName}_e", Option("default")), sqlContext.sparkSession))
    assert(new File(tablePath).exists())
  }

  test("test truncate table") {
    val tableName = "tbl_truncate"
    sql(s"create table ${tableName} using carbondata as select * from origin_csv")
    checkAnswer(sql(s"select count(*) from ${tableName}"), Seq(Row(3)))
    sql(s"truncate table ${tableName}")
    checkAnswer(sql(s"select count(*) from ${tableName}"), Seq(Row(0)))
  }

  def createDataSourcePartitionTable(provider: String, tableName: String): Unit = {
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName}(col1 int, col2 string) using $provider partitioned by (col2)")
    checkLoading(s"${tableName}")
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"),tableName)(sqlContext.sparkSession)
    val isHivePartitionTable = carbonTable.isHivePartitionTable
    sql(s"describe formatted ${tableName}").show(100, false)
    sql(s"show partitions ${tableName}").show(100, false)
    sql(s"show create table ${tableName}").show(100, false)
  }

  def createHivePartitionTable(provider: String, tableName: String): Unit = {
    sql(s"drop table if exists ${tableName}")
    sql(s"create table ${tableName}(col1 int) partitioned by (col2 string) stored as carbondata")
    checkLoading(s"${tableName}")
    sql(s"describe formatted ${tableName}").show(100, false)
    sql(s"show partitions ${tableName}").show(100, false)
  }

  def verifyDataSourceTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(col1 int, col2 string) using $provider")
    checkLoading(tableName)
    sql(s"create table ${tableName}_ctas using $provider as select * from ${tableName}")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc")))
    sql(s"insert into ${tableName}_ctas select 123, 'abc'")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc"), Row(123, "abc")))
  }

  def verifyHiveTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(col1 int, col2 string) stored as $provider")
    checkLoading(tableName)
    sql(s"create table ${tableName}_ctas stored as $provider as select * from ${tableName}")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc")))
    sql(s"insert into ${tableName}_ctas select 123, 'abc'")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc"), Row(123, "abc")))
  }

  def checkLoading(tableName: String): Unit = {
    sql(s"insert into $tableName select 123, 'abc'")
    checkAnswer(sql(s"select * from $tableName"),
      Seq(Row(123, "abc")))
  }

}