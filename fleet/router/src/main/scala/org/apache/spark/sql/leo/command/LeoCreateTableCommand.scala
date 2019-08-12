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

import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, RunnableCommand}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory

/**
 * non PK Table
 * @param table
 * @param ignoreIfExists
 */
case class LeoCreateTableCommand(
    table: CatalogTable,
    ignoreIfExists: Boolean)
  extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val db: String = table.database
  val tableName: String = table.identifier.table

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check whether the table exists
    if (CarbonEnv.isTableExists(table.identifier)(sparkSession)) {
      if (ignoreIfExists) return Seq.empty
      else throw new AnalysisException("table " + table.identifier + " already exists")
    }
    createNPKTable(sparkSession)

  }

  private def createNPKTable(sparkSession: SparkSession): Seq[Row] = {
    // step 1: create table in carbon (persist schema file)
    val updatedCatalog = try {
      CarbonSource.updateCatalogTableWithCarbonSchema(table, sparkSession)
        .copy(provider = Some("org.apache.spark.sql.CarbonSource"))
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        throw e
    }

    // step 2: save meta in metastore, undo previous steps if failed
    try {
      val cmd = CreateDataSourceTableCommand(updatedCatalog, ignoreIfExists)
      cmd.run(sparkSession)
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        deleteCarbonTableFolder(table)
        throw e
    }
  }

  private def deleteCarbonTableFolder(table: CatalogTable): Boolean = {
    val path = table.properties("tablePath")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(path))
  }
}
