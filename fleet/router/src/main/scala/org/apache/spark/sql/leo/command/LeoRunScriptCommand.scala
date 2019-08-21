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

package org.apache.spark.sql.leo.command

import java.util.NoSuchElementException

import scala.io.Source

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.pythonudf.PythonUDFRegister
import org.apache.spark.sql.types.StringType


/**
 * Run the given script by sending one task to executor and run the script.
 * This is done by executing a UDF against a temporary table
 *
 * @param scriptPath
 * @param funcName
 * @param params
 * @param outputNames
 */
case class LeoRunScriptCommand(
    scriptPath: String,
    funcName: String,
    params: Map[String, String],
    outputNames: Map[String, String])
  extends RunnableCommand {

  // the script to run in executor
  private def script: String = {
    val source = Source.fromFile(scriptPath)
    val content = source.mkString
    source.close()
    val paramList = params.map{x => s"${x._1}=${x._2}"}.mkString(",")
    s"""
       |$content
       |def foo(i):
       |  x = $funcName($paramList)
       |  return x
     """.stripMargin
  }

  override def output: Seq[Attribute] = {
    if (outputNames.nonEmpty) {
      outputNames.map { x =>
        AttributeReference(x._1, CatalystSqlParser.parseDataType(x._2))()
      }.toSeq
    } else {
      Seq(AttributeReference("value", StringType)())
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    if (output.length > 1) {
      throw new AnalysisException("output fields should be less than 2")
    }
    val tempDBNameInConf = spark.conf.get("leo.temp.db", "")
    val needCreateTempDB = tempDBNameInConf.equalsIgnoreCase("")
    // if has temp db name in conf, we use it and don't drop it, otherwise we should create a
    // temp one finally clean it.
    val tempDBName =
      if(needCreateTempDB) "tempdb" + System.nanoTime() else tempDBNameInConf

    val tempTableName = "temptable" + System.nanoTime()
    val tempFuncName = "tempfunc" + System.nanoTime()

    // create a temporary UDF and use it in an temporary table with one record
    // so the UDF will be executed once in one task only
    PythonUDFRegister.registerPythonUDF(
      spark,
      tempFuncName,
      "foo",
      script,
      Array[String](),
      output.head.dataType)

    val rows = try {
      if (needCreateTempDB) {
        spark.sql(s"create database if not exists $tempDBName")
      }
      spark.sql(s"create view $tempDBName.$tempTableName as select 1")
      spark.sql(s"select $tempFuncName(1) from $tempDBName.$tempTableName").collect()
    } finally {
      spark.sql(s"drop view if exists $tempDBName.$tempTableName")
      if (needCreateTempDB) {
        spark.sql(s"drop database if exists $tempDBName")
      }
      PythonUDFRegister.unregisterPythonUDF(spark, tempFuncName)
    }
    rows
  }
}
