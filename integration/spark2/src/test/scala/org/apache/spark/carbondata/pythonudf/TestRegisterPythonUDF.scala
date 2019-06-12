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
package org.apache.spark.carbondata.pythonudf

import org.apache.spark.sql.pythonudf.PythonUDFRegister
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.LongType
import org.scalatest.BeforeAndAfterEach

/**
 *
 */
class TestRegisterPythonUDF extends QueryTest with BeforeAndAfterEach {

  // Set the SPARK_HOME in environmental variables to execute this testcase.
  test("register pythonUDF test") {

    val script =
      """
        |def square(s):
        |  return s * s
      """.stripMargin

    val squared = (s: Long) => {
      s * s
    }

    new PythonUDFRegister()
      .registerPythonUDF(sqlContext.sparkSession,
        "square",
        "square",
        script,
        Array[String](),
        LongType)
    sqlContext.sparkSession.udf.register("scalasquare", squared)
    sqlContext.sparkSession.range(1, 20).registerTempTable("test")
    checkAnswer(sql("select id, square(id) as id_squared from test where id=9"),
      sql("select id, scalasquare(id) as id_squared from test where id=9"))
  }


}
