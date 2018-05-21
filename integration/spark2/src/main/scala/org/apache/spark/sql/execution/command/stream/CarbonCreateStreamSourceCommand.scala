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

package org.apache.spark.sql.execution.command.stream

import scala.collection.mutable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{Field, MetadataCommand, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

/**
 * This command is used to create Stream Source, which is implemented as a Carbon Table
 */
case class CarbonCreateStreamSourceCommand(
    dbName: Option[String],
    tableName: String,
    fields: Seq[Field],
    tblProperties: Map[String, String]
) extends MetadataCommand {
  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val tableModel = new CarbonSpark2SqlParser().prepareTableModel(
      ifNotExistPresent = false,
      dbName,
      tableName,
      fields,
      Seq.empty,
      mutable.Map[String, String](tblProperties.toSeq: _*),
      None
    )
    val tableInfo = TableNewProcessor.apply(tableModel)
    val command = CarbonCreateTableCommand(tableInfo)
    command.run(sparkSession)
  }
}
