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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable

// create database should create OBS bucket
case class LeoCreateDatabaseCommand(sparkCommand: CreateDatabaseCommand)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }
}

// create database should create OBS bucket
case class LeoDropDatabaseCommand(sparkCommand: DropDatabaseCommand)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }
}

// create database should create OBS bucket
case class LeoCreateTableCommand(
    sparkCommand: CreateTable,
    tableDesc: CatalogTable,
    mode: SaveMode)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }
}

// create database should create OBS bucket
case class LeoDropTableCommand(
    sparkCommand: DropTableCommand,
    ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    dropChildTable: Boolean = false)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }
}
