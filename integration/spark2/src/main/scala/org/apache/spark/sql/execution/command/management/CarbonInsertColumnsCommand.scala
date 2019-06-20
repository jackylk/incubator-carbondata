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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, Auditable,
BucketFields, Field, RunnableCommand, TableNewProcessor}
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableAddColumnCommand
import org.apache.spark.sql.execution.command.vector.InsertColumnsHelper
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}

/**
 * based same table to insert a new column as the following steps:
 * 1. generate a virtual column, not add to the metadata of the table
 * 2. insert the data for this virtual column
 * 3. add the virtual column into the table
 */
case class CarbonInsertColumnsCommand(
    field: Field,
    dbName: Option[String],
    tableName: String,
    query: String)
  extends RunnableCommand with Auditable {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val table = CarbonEnv.getCarbonTable(dbName, tableName)(sparkSession)
    if (!table.isVectorTable) {
      throw new UnsupportedOperationException("insert columns only support vector table")
    }
    if (table.getColumnByName(table.getTableName, field.column) != null) {
      throw new MalformedCarbonCommandException(
        s"column name: ${ field.column } already used by this table")
    }
    val plan = sparkSession.sql(query)
    setAuditTable(table)
    // 1. generate a virtual column
    val (column, columnSchemas) = generateVirtualColumn()
    // 2. insert columns data into each segments
    InsertColumnsHelper.insertColumnsForVectorTable(
      sparkSession,
      table,
      column,
      plan,
      sparkSession.sessionState.newHadoopConf()
    )
    // 3. add columns into table schema
    addColumnIntoTable(sparkSession, table, columnSchemas, column.isDimension)
    Seq.empty
  }

  /**
   * generate a virtual column for insert columns function
   * it is not in the table schema now.
   * @return CarbonDimension or CarbonMeasure
   * @return Seq[ColumnSchema]
   */
  def generateVirtualColumn(): (CarbonColumn, Seq[ColumnSchema]) = {
    // TODO need to infer data type for abstract complex data type
    val parser = new CarbonSpark2SqlParser()
    val virtualTableModel = parser.prepareTableModel(
      true,
      Option("default"),
      "default",
      Seq(field),
      Seq.empty,
      scala.collection.mutable.Map.empty[String, String],
      Option.empty[BucketFields],
      false,
      false,
      None)
    val virtualTableInfo = TableNewProcessor(virtualTableModel)
    val virtualTable = CarbonTable.buildFromTableInfo(virtualTableInfo)
    val virtualColumn =
      virtualTable
        .getCreateOrderColumn(virtualTable.getTableName)
        .get(0)
    val virtualSchemas =
      virtualTable
        .getTableInfo
        .getFactTable
        .getListOfColumns
        .asScala
        .filter(!_.isInvisible)
    (virtualColumn, virtualSchemas)
  }

  /**
   * add new columns into the this table
   * @param sparkSession
   * @param table
   * @param columnSchemas
   * @param isDimension
   */
  def addColumnIntoTable(
      sparkSession: SparkSession,
      table: CarbonTable,
      columnSchemas: Seq[ColumnSchema],
      isDimension: Boolean): Unit = {
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
