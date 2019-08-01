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

package org.apache.spark

import org.apache.spark.sql.leo.VisionSparkUDFs
import org.apache.spark.sql.pythonudf.PythonUDFRegister
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

class TestExtractVideoFrames extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sourcetable")
  }

  test("test extract-frame-from-videos udf") {

    // Path to data folder containing videos
    val dataDirPath = "/huawei/naman/code/data/video/"

    // Path to scripts folder, containing the extra scripts. It will be added to sys.path
    val scriptsDirPath = "../../fleet/vision/src/main/scala/org/apache/spark/sql/leo/video/"

    sql("DROP TABLE IF EXISTS sourcetable")
    sql("CREATE TEMPORARY TABLE sourcetable USING binaryfile OPTIONS(path='" + dataDirPath + "')")

    VisionSparkUDFs.registerExtractFramesFromVideo(sqlContext.sparkSession, scriptsDirPath)

    sql(
      s"""
         | select
         |   path as File,
         |   generate_x_frames_per_sec(path, 5, '/tmp/video') as `Success, Num-Files,  Time Taken (s)`
         | from sourcetable
        """.stripMargin).show(100, false)
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sourcetable")
  }
}


