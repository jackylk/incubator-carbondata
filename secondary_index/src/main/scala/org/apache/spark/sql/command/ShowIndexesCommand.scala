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

package org.apache.spark.sql.command

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.CarbonInternalMetastore
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

/**
 * Command to list the indexes for a table
 */
private[sql] case class ShowIndexes(
    databaseNameOp: Option[String],
    tableName: String,
    var inputSqlString: String = null,
    override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    // Here using checkSchemasModifiedTimeAndReloadTables in tableExists to reload metadata if
    // schema is changed by other process, so that tableInfoMap woulb be refilled.
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val identifier = TableIdentifier(tableName, databaseNameOp)
    val tableExists = catalog
      .tableExists(identifier)(sparkSession)
    if (!tableExists) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val carbonTable = catalog.lookupRelation(Some(databaseName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    if (carbonTable == null) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
    if (null == indexesMap) {
      throw new Exception("Secondary index information is not loaded in main table")
    }
    val indexTableMap = indexesMap.asScala
    if (indexTableMap.nonEmpty) {
      val indexList = indexTableMap.map { indexInfo =>
        (indexInfo._1, indexInfo._2.asScala.mkString(","))
      }
      indexList.map { case (indexTableName, columnName) =>
        Row(f"$indexTableName%-30s $columnName")
      }.toSeq
    } else {
      Seq.empty
    }
  }

}
