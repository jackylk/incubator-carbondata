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

package org.apache.carbondata.spark.testsuite.sdk

import scala.io.Source

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.memory.CarbonUnsafe
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.arrow.ArrowConverter
import org.apache.carbondata.sdk.file.{ArrowCarbonReader, CarbonReader, CarbonSchemaReader, CarbonWriter}

class TestOnlineSegmentAPP extends QueryTest {

  test("test create table with online segment and write & read data through SDK") {
    // 1. create table
    sql("drop table if exists test")
    sql(
      s"""
         | CREATE TABLE test (empno int, empname string, designation String, doj Timestamp,
         | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
         | utilization int,salary int)
         | STORED AS carbondata
      """.stripMargin)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

    // 2. create segment and get segmentPath
    val carbonTable = CarbonEnv
      .getCarbonTable(Some("default"), "test")(sqlContext.sparkSession)

    val segmentManager = carbonTable.getSegmentManager
    val segmentId = segmentManager.createSegment(FileFormat.COLUMNAR_V3)

    val segmentPath = carbonTable.getSegmentPath(segmentId)

    // 3. write data through CarbonWriter SDK
    val carbonWriterBuilder = CarbonWriter.builder()

    val schemaFilePath = carbonTable.getMetadataPath + "/schema"

    val writer = carbonWriterBuilder.outputPath(segmentPath)
      .uniqueIdentifier(System.currentTimeMillis).withBlockSize(2)
      .withSchemaFile(schemaFilePath)
      .withCsvInput()
      .writtenBy("TestOnlineSegmentAPP").build()

    val source = Source.fromFile(s"$resourcesPath/data.csv")
    var count = 0
    for (line <- source.getLines()) {
      if (count != 0) {
        writer.write(line.split(","))
      }
      count = count + 1
    }
    writer.close()
    // 4. Commit Segment
    segmentManager.commitSegment(segmentId)

    checkAnswer(sql("select count(*) from test"), Seq(Row(10)))

    sql("select * from test").show(false)
    sql("drop table if exists test")
  }

  test("test online segment app") {
    sql("drop table if exists test")
    sql(
      s"""
         | CREATE TABLE test (empno int, empname string, designation String, doj Timestamp,
         | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
         | utilization int,salary int)
         | STORED AS carbondata
      """.stripMargin)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

    // create segment and get segmentPath
    val carbonTable = CarbonEnv
      .getCarbonTable(Some("default"), "test")(sqlContext.sparkSession)
    val segmentManager = carbonTable.getSegmentManager
    val segmentId = segmentManager.createSegment(FileFormat.COLUMNAR_V3)

    var segmentPath = carbonTable.getSegmentPath(segmentId)
    val schemaFilePath = carbonTable.getMetadataPath + "/schema"
    try {

      var count = 0
      // when the passing online segment is full, trigger 'handoff' operation, create and return a
      // new online segment path. Otherwise return the passing path
      while (count <= 100) {

        segmentPath = segmentManager.getOrCreateSegment(segmentPath, FileFormat.COLUMNAR_V3)

        val carbonWriterBuilder = CarbonWriter.builder()

        val writer = carbonWriterBuilder.outputPath(segmentPath)
          .uniqueIdentifier(System.currentTimeMillis).withBlockSize(2)
          .withSchemaFile(schemaFilePath)
          .withCsvInput().writtenBy("TestOnlineSegmentAPP").build()
        val source = Source.fromFile(s"$resourcesPath/data.csv")
        var header = 0
        for (line <- source.getLines()) {
          if (header != 0) {
            writer.write(line.split(","))
          }
          header = header + 1
        }
        writer.close()
        count += 1
      }
      // commit the last segment
      segmentManager.commitSegment(CarbonTablePath.getSegmentId(segmentPath))
    } catch {
      case e: Exception =>
        segmentManager.deleteSegment(CarbonTablePath.getSegmentId(segmentPath))
    }

    // read data written to online segments through carbon reader
    val value: ArrowCarbonReader[Array[Object]] =
      CarbonReader.builder(carbonTable.getTablePath, carbonTable.getTableName).buildArrowReader()

    val carbonSchema = CarbonSchemaReader.readSchema(carbonTable.getTablePath)

    val address = value.readArrowBatchAddress(carbonSchema)
    val length = CarbonUnsafe.getUnsafe.getInt(address)
    val data1 = new Array[Byte](length)
    CarbonUnsafe.getUnsafe
      .copyMemory(null, address + 4, data1, CarbonUnsafe.BYTE_ARRAY_OFFSET, length)
    val arrowConverter1 = new ArrowConverter(carbonSchema, 0)
    val rowcount = arrowConverter1.byteArrayToVector(data1).getRowCount
    value.close()
    sql("select * from test").show(false)
    sql("select count(*) from test").show(false)
    checkAnswer(sql("select count(*) from test"), Seq(Row(rowcount)))
  }

  test("test online segment app failing case") {
    sql("drop table if exists test")
    sql(
      s"""
         | CREATE TABLE test (empno int, empname string, designation String, doj Timestamp,
         | workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         | projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
         | utilization int,salary int)
         | STORED AS carbondata
      """.stripMargin)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

    // create segment and get segmentPath
    val carbonTable = CarbonEnv
      .getCarbonTable(Some("default"), "test")(sqlContext.sparkSession)
    val segmentManager = carbonTable.getSegmentManager
    val segmentId = segmentManager.createSegment(FileFormat.COLUMNAR_V3)

    var segmentPath = carbonTable.getSegmentPath(segmentId)
    val schemaFilePath = carbonTable.getMetadataPath + "/schema"
    try {

      var count = 0
      // when the passing online segment is full, trigger 'handoff' operation, create and return a
      // new online segment path. Otherwise return the passing path
      while (count <= 3) {

        segmentPath = segmentManager.getOrCreateSegment(segmentPath, FileFormat.COLUMNAR_V3)

        val carbonWriterBuilder = CarbonWriter.builder()

        val writer = carbonWriterBuilder.outputPath(segmentPath)
          .uniqueIdentifier(System.currentTimeMillis).withBlockSize(2)
          .withSchemaFile(schemaFilePath)
          .withCsvInput().writtenBy("TestOnlineSegmentAPP").build()
        val source = Source.fromFile(s"$resourcesPath/data.csv")
        var header = 0
        for (line <- source.getLines()) {
          if (header != 0) {
            writer.write(line.split(","))
            if (count == 2) {
              throw new Exception("fail")
            }
          }
          header = header + 1
        }
        writer.close()
        count += 1
      }
      // commit the last segment
      segmentManager.commitSegment(CarbonTablePath.getSegmentId(segmentPath))
    } catch {
      case e: Exception =>
        segmentManager.deleteSegment(CarbonTablePath.getSegmentId(segmentPath))
    }
    sql("select * from test").show(false)
    sql("select count(*) from test").show(false)
    checkAnswer(sql("select count(*) from test"), Seq(Row(20)))
  }
}
