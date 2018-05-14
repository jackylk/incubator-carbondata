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

package org.apache.spark.carbondata.commands

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Class to test carbon.load.datamaps.parallel. configuration
 */
class LoadDataMapsParallelSETCommandTestCase extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop database if exists set_command cascade")
    sql("create database set_command")
    sql("use set_command")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table_par")
    sql("DROP TABLE IF EXISTS carbon_table_par1")
    sql("CREATE TABLE IF NOT EXISTS carbon_table (empno String, doj String, salary Int) STORED BY " +
        "'org.apache.carbondata.format'")
    val csvFilePath = s"$resourcesPath/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    sql("CREATE TABLE IF NOT EXISTS carbon_table1 (empno String, doj String, salary Int) STORED BY " +
        "'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table1 OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");

    sql("CREATE TABLE IF NOT EXISTS carbon_table_par (doj String, salary Int) PARTITIONED BY (empno String) STORED BY " +
        "'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table_par OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    sql("CREATE TABLE IF NOT EXISTS carbon_table_par1 (doj String, salary Int) PARTITIONED BY (empno String) STORED BY " +
        "'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table_par1 OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
  }

  test("test SET carbon.load.datamaps.parallel. after load") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=true")
    checkAnswer(sql("select * from carbon_table"), sql("select * from carbon_table1"))
  }

  test("test SET carbon.load.datamaps.parallel. after update") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=false")
    sql("update carbon_table set(empno)=('21') where empno='11'").show()
    sql("update carbon_table1 set(empno)=('21') where empno='11'").show()
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=true")
    checkAnswer(sql("select * from carbon_table"), sql("select * from carbon_table1"))
  }

  test("test SET carbon.load.datamaps.parallel. for partition table after load") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=true")
    checkAnswer(sql("select * from carbon_table_par"), sql("select * from carbon_table_par1"))
  }

  test("test SET carbon.load.datamaps.parallel. for partition table after update") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=false")
    sql("update carbon_table_par set(salary)=('20000') where salary is null").show()
    sql("update carbon_table_par1 set(salary)=('20000') where salary is null").show()
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=true")
    checkAnswer(sql("select * from carbon_table_par"), sql("select * from carbon_table_par1"))
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table_par")
    sql("DROP TABLE IF EXISTS carbon_table_par1")
    sql("drop database if exists set_command cascade")
  }
}
