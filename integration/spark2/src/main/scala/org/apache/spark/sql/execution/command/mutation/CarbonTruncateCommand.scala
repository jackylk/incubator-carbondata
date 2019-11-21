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

package org.apache.spark.sql.execution.command.mutation

import java.util

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.{DataCommand, TruncateTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}

case class CarbonTruncateCommand(child: TruncateTableCommand) extends DataCommand {
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(child.tableName.database)(sparkSession)
    val tableName = child.tableName.table
    setAuditTable(dbName, tableName)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      throw new NoSuchTableException(dbName, tableName)
    }
    if (null == relation.carbonTable) {
      LOGGER.error(s"Truncate table failed. table not found: $dbName.$child.tableName.table")
      throw new NoSuchTableException(dbName, child.tableName.table)
    }
    if (child.partitionSpec.isDefined) {
      throw new MalformedCarbonCommandException(
        "Unsupported truncate table with specified partition")
    }
    // select an empty result set for get schema.
    val tableSchema = sparkSession.sql(s"SELECT * FROM $dbName.$tableName WHERE false").schema
    val tableColumnNamesBuilder = new StringBuilder
    for (tableField <- tableSchema.fields) {
      tableColumnNamesBuilder.append(tableField.name).append(',')
    }
    // overwrite table with a empty data set.
    CarbonLoadDataCommand(
      databaseNameOp = Option(dbName),
      tableName = tableName,
      factPathFromUser = null,
      dimFilesPath = Seq.empty,
      options = Map((
        "fileheader",
        tableColumnNamesBuilder.substring(0, tableColumnNamesBuilder.length() - 1)
      )),
      isOverwriteTable = true,
      dataFrame = Option(sparkSession.createDataFrame(
        new util.ArrayList[Row](),
        tableSchema)
      )
    ).run(sparkSession)
    Seq.empty
  }

  override protected def opName: String = "TRUNCATE TABLE"
}
