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

package org.apache.spark.sql.execution.command.table

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{MetadataCommand, ShowCreateTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.logging.LogServiceFactory

case class CarbonShowCreateTableCommand(
    child: ShowCreateTableCommand
) extends MetadataCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override protected def opName: String = "SHOW CREATE TABLE"

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(child.table.database)(sparkSession)
    val tableName = child.table.table
    setAuditTable(dbName, tableName)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      throw new NoSuchTableException(dbName, tableName)
    }
    if (null == relation.carbonTable) {
      LOGGER.error(s"Show create table failed. table not found: $dbName.$child.tableName.table")
      throw new NoSuchTableException(dbName, child.table.table)
    }
    if (!CarbonEnv.isPrivacy(sparkSession, relation.carbonTable.isExternalTable)) {
      child.run(sparkSession)
    } else {
      var inOptions = false
      val createSql = child
        .run(sparkSession)
        .head
        .getString(0)
        .split("\n", -1)
        .map { row =>
          if (inOptions) {
            if (row.startsWith("  `tablepath`")) {
              null
            } else {
              if (row.equals(")")) {
                inOptions = false
              }
              row
            }
          } else {
            if (row.equals("OPTIONS (")) {
              inOptions = true
            }
            row
          }
        }
        .filter(_ != null)
        .mkString("\n")
      Seq(Row(createSql))
    }
  }
}
