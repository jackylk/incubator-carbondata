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

package org.apache.spark.sql.execution.command.management

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, Auditable, Field, RunnableCommand, TableNewProcessor}
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableAddColumnCommand
import org.apache.spark.sql.execution.command.vector.InsertColumnsHelper

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema

/**
 * based same table to insert a new column as the following steps:
 * 1. insert the data
 * 2. add the column into the table
 */
case class CarbonInsertColumnsCommand(
    fields: Seq[Field],
    dbName: Option[String],
    tableName: String,
    query: String)
  extends RunnableCommand with Auditable {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = CarbonEnv.getCarbonTable(dbName, tableName)(sparkSession)
    validate(table)
    val inputData = sparkSession.sql(query)
    setAuditTable(table)
    // 1. insert columns data into each segments
    val columnSchemas =
      InsertColumnsHelper.insertColumnsForVectorTable(
        sparkSession,
        table,
        fields,
        inputData,
        sparkSession.sessionState.newHadoopConf())
    // 3. add columns into table schema
    addColumnIntoTable(sparkSession, table, columnSchemas)
    Seq.empty
  }

  def validate(table: CarbonTable): Unit = {
    if (!table.isVectorTable) {
      throw new MalformedCarbonCommandException("insert columns only support vector table")
    }

    if (fields.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"insert column list can not be empty")
    }

    val uniqueSize = fields.map(_.column.toLowerCase).toSet.size
    if (uniqueSize != fields.size) {
      throw new MalformedCarbonCommandException(
        s"not allow multiple columns have a same name")
    }

    fields.foreach { field =>
      if (field.column.contains(".")) {
        throw new MalformedCarbonCommandException(
          s"the column name: ${ field.column } can not contain a dot")
      }
      if (table.getColumnByName(table.getTableName, field.column) != null) {
        throw new MalformedCarbonCommandException(
          s"column name: ${ field.column } already used by this table")
      }
    }

    if (!query.toLowerCase.trim.startsWith("select")) {
      throw new MalformedCarbonCommandException(
        s"insert columns should be followed by a select sql")
    }
  }

  /**
   * add new columns into the this table
   * @param sparkSession
   * @param table
   * @param columnSchemas
   */
  def addColumnIntoTable(
      sparkSession: SparkSession,
      table: CarbonTable,
      columnSchemas: Seq[ColumnSchema]
  ): Unit = {
    val alterTableAddColumnsModel =
      AlterTableAddColumnsModel(
        Option(table.getDatabaseName),
        table.getTableName,
        Map.empty,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        columnSchemas)
    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel).run(sparkSession)
  }

  override protected def opName = "INSERT COLUMNS"
}
