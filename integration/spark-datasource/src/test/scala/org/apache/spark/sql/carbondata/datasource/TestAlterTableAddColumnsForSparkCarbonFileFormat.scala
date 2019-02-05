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
package org.apache.spark.sql.carbondata.datasource

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.carbondata.datasource.TestUtil.spark
import org.apache.spark.util.SparkUtil
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestAlterTableAddColumnsForSparkCarbonFileFormat extends FunSuite with BeforeAndAfterAll {


  test("alter table add column write through df") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      // Saves dataframe to carbon file
      df.write
        .format("carbon").saveAsTable("testformat")
      spark.sql("alter table testformat add columns(name string)")
      assert(spark.sql("select * from testformat where name is null").count() == 10)
      spark.sql("insert into testformat select 'abc','def',4,'aaa'")
      spark.sql("select * from testformat where name = 'aaa'").show()
      assert(spark.sql("select * from testformat where name = 'aaa'").count() == 1)
    }
  }

  test("test write using ddl") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      // Saves dataframe to carbon file
      df.write
        .format("parquet").saveAsTable("testparquet")
      spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
      spark.sql("insert into carbon_table select * from testparquet")
      spark.sql("alter table testparquet add columns(name string, age int)")
      spark.sql("alter table carbon_table add columns(name string)")
      spark.sql("alter table carbon_table add columns(age int)")
      TestUtil
        .checkAnswer(spark.sql("select * from carbon_table where name is null"),
          spark.sql("select * from testparquet where name is null"))
      TestUtil
        .checkAnswer(spark.sql("select * from carbon_table"),
          spark.sql("select * from testparquet"))
      TestUtil
        .checkAnswer(spark.sql("select * from carbon_table where name != 'chandler'"),
          spark.sql("select * from testparquet  where name != 'chandler'"))
    }
  }

  test("test insert data after add columns for less number of columns") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      // Saves dataframe to carbon file
      df.write
        .format("carbon").saveAsTable("carbon_table")
      spark.sql("alter table carbon_table add columns(name string, age int)")
      val ex = intercept[AnalysisException] {
        spark.sql("insert into carbon_table select 'abc','def',4")
      }
      assert(ex.getMessage()
        .contains(
          "data to be inserted have the same number of columns as the target table: target table " +
          "has 5 column(s) but the inserted data has 3 column(s), including 0 partition column(s) " +

          "having constant value(s)"))
    }
  }

  test("test adding existing columns and case sensitive check") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
      val ex1 = intercept[AnalysisException] {
        spark.sql("alter table carbon_table add columns(C1 string)")
      }
      assert(ex1.getMessage()
        .contains("Found duplicate column(s) in the table definition of carbon_table: `c1`"))
      val ex2 = intercept[AnalysisException] {
        spark.sql("alter table carbon_table add columns(c1 string)")
      }
      assert(ex2.getMessage()
        .contains("Found duplicate column(s) in the table definition of carbon_table: `c1`"))
    }
  }

  test("test alter add columns on partition tables") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      import spark.implicits._
      val df = spark.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      // Saves dataframe to carbon file
      df.write
        .format("carbon").partitionBy("c2").saveAsTable("carbon_table")
      spark.sql("alter table carbon_table add columns(name string, salary double)")
      spark.sql("insert into carbon_table select 'abc',4,'test',2000.0,'rg'")
      assert(spark.sql("select * from carbon_table where salary = 2000").count() == 1)
    }
  }

  test("test alter add columns with column comment") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      spark.sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
      spark.sql("alter table carbon_table add columns(name string COMMENT 'this is new column')")
      val df = spark.sql("desc carbon_table").collect
      df.find(_.get(0).toString.contains("name")) match {
        case Some(row) => assert(row.get(2).toString.contains("this is new column"))
        case None => assert(false)
      }
    }
  }

  test("test add columns of different datatype") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      spark.sql("create table carbon_table (name string, age int) using carbon")
      spark.sql("insert into carbon_table select 'name',4")
      spark.sql("alter table carbon_table add columns(salary float, avg double)")
      spark.sql("insert into carbon_table select 'sd',6,345.543,432.56789")
      assert(spark.sql("select * from carbon_table where avg = '432.56789'").count() == 1)
      assert(spark.sql("select * from carbon_table where salary = '345.543'").count() == 1)
      assert(spark.sql("select * from carbon_table where salary is null").count() == 1)
    }
  }

  test("test add columns for complex types") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      spark.sql("drop table if exists carbon_table")
      spark.sql("create table carbon_table (stringfield string, structfield struct<bytefield: " +
                "byte, floatfield: float>) using carbon")
      spark.sql("alter table carbon_table add columns(com struct<intfield: int>)")
      spark.sql("select * from carbon_table").show()
    }
  }

  override protected def beforeAll(): Unit = {
    drop
  }

  override def afterAll(): Unit = {
    drop
  }

  private def drop = {
    spark.sql("drop table if exists testformat")
    spark.sql("drop table if exists carbon_table")
    spark.sql("drop table if exists testcarbon")
    spark.sql("drop table if exists testparquet")
  }

}
