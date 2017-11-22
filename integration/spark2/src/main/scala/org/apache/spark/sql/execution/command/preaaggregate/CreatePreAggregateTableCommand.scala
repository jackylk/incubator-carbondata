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
package org.apache.spark.sql.execution.command.preaaggregate

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}

/**
 * Below command class will be used to create pre-aggregate table
 * and updating the parent table about the child table information
 * Failure case:
 * 1. failed to create pre aggregate table.
 * 2. failed to update main table
 *
 * @param queryString
 */
case class CreatePreAggregateTableCommand(
    dataMapName: String,
    parentTableIdentifier: TableIdentifier,
    dmClassName: String,
    dmproperties: Map[String, String],
    queryString: String)
  extends RunnableCommand with SchemaProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val df = sparkSession.sql(updatedQuery)
    val dataMapFields = PreAggregateUtil.validateActualSelectPlanAndGetAttributes(
      df.logicalPlan, queryString)
    val dataMapSchema = dataMapFields.map(entry => (entry._1.getFieldName, entry._2)).toMap
    val fields = dataMapFields.keySet.toList
    val tableProperties = mutable.Map[String, String]()
    dmproperties.foreach(t => tableProperties.put(t._1, t._2))
    // Create the aggregation table name with parent table name prefix
    val tableIdentifier = TableIdentifier(
        parentTableIdentifier.table +"_" + dataMapName, parentTableIdentifier.database)

    val databaseName = CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession)
    val tableName = tableIdentifier.table

    // getting the parent table
    val parentTable = PreAggregateUtil.getParentCarbonTable(df.logicalPlan)
    // getting the table name
    val parentTableName = parentTable.getTableName
    // getting the db name of parent table
    val parentDbName = parentTable.getDatabaseName

    assert(parentTableName.equalsIgnoreCase(parentTableIdentifier.table))

    CarbonCreateTableCommand(
      databaseNameOp = tableIdentifier.database,
      tableName = tableIdentifier.table,
      tableProperties = tableProperties,
      tableSchema = CarbonDataTypes.createStructType(fields.asJava),
      parentTable = Some(parentTable),
      dataMapFields = Some(dataMapSchema)
    ).run(sparkSession)

    try {
      val table = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
      val tableInfo = table.getTableInfo
      // child schema object which will be updated on parent table about the
      val childSchema = tableInfo.getFactTable
        .buildChildSchema(dataMapName, CarbonCommonConstants.AGGREGATIONDATAMAPSCHEMA,
        tableInfo.getDatabaseName, queryString, "AGGREGATION")
      dmproperties.foreach(f => childSchema.getProperties.put(f._1, f._2))
      // updating the parent table about child table
      PreAggregateUtil.updateMainTable(parentDbName, parentTableName, childSchema, sparkSession)
      val loadAvailable = PreAggregateUtil.checkMainTableLoad(parentTable)
      if (loadAvailable) {
        sparkSession.sql(
          s"insert into $databaseName.$tableName $queryString")
      }
    } catch {
      case e: Exception =>
        CarbonDropTableCommand(
          ifExistsSet = true,
          Some(databaseName),
          tableName
        ).run(sparkSession)
        throw e

    }
    Seq.empty
  }
}


