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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for all data source
  *
  */
class AllDataSourceTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    dropTable
  }

  def dropTable {
    dropTableByName("ds_carbon")
    dropTableByName("ds_carbondata")
    dropTableByName("ds_hive_carbon")
    dropTableByName("ds_hive_carbondata")
  }

  def dropTableByName(tableName: String) :Unit = {
    sql(s"drop table if exists $tableName")
    sql(s"drop table if exists ${tableName}_p")
    sql(s"drop table if exists ${tableName}_ctas")
  }

  override def afterAll: Unit = {
    dropTable
  }

  test("test carbon"){
    verifyDataSourceTable("carbon", "ds_carbon")
    verifyHiveTable("carbon", "ds_hive_carbon")
  }

  test("test carbondata"){
    verifyDataSourceTable("carbondata", "ds_carbondata")
    verifyHiveTable("carbondata", "ds_hive_carbondata")
  }

  def verifyDataSourceTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(key int, value string) using $provider")
    checkLoading(tableName)

    sql(s"create table ${tableName}_p(key int, value string) using $provider partitioned by (value)")
    checkLoading(s"${tableName}_p")

    sql(s"create table ${tableName}_ctas using $provider as select * from ${tableName}")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc")))
    sql(s"insert into ${tableName}_ctas select 123, 'abc'")
    checkAnswer(sql(s"select * from ${tableName}_ctas"),
      Seq(Row(123, "abc"), Row(123, "abc")))
  }

  def verifyHiveTable(provider: String, tableName: String): Unit = {
    sql(s"create table ${tableName}(key int, value string) stored as $provider")
    checkLoading(tableName)

    sql(s"create table ${tableName}_p(key int) partitioned by (value string) stored as $provider ")
    checkLoading(s"${tableName}_p")

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