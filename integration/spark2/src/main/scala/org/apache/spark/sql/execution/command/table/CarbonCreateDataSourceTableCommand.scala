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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{CarbonSource, Row, SparkSession}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, MetadataCommand}

/**
 * this command wrap schema generation and CreateDataSourceTableCommand
 * step 1: generate schema file
 * step 2: create table
 */
case class CarbonCreateDataSourceTableCommand(
    table: CatalogTable,
    ignoreIfExists: Boolean
) extends MetadataCommand {
  override protected def opName: String = "CREATE TABLE"

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    new CreateDataSourceTableCommand(
      CarbonSource.updateCatalogTableWithCarbonSchema(table, sparkSession),
      ignoreIfExists
    ).run(sparkSession)
  }
}
