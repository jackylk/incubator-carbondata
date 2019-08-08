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

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{DropDatabaseCommand, DropTableCommand, RunnableCommand}
import org.apache.spark.sql.leo.LeoEnv
import org.apache.spark.sql.leo.util.OBSUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil

case class LeoDropDatabaseCommand(command: DropDatabaseCommand)
  extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override val output: Seq[Attribute] = command.output
  val dbName : String = command.databaseName
  val bucket: String = LeoEnv.bucketName(dbName)
  val ifExists : Boolean = command.ifExists

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // step 1. check whether the input db exists
    val databaseLocation = try {
      CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    } catch {
      case e: NoSuchDatabaseException =>
        // if database not found and ifExists true return empty
        if (command.ifExists) {
          return Seq.empty
        } else {
          throw e
        }
    }

    // step 2. drop all tables if cascaded is set
    val tablesInDB: Seq[TableIdentifier] =
      if (sparkSession.sessionState.catalog.listDatabases().exists(_.equalsIgnoreCase(dbName))) {
        sparkSession.sessionState.catalog.listTables(dbName)
      } else {
        Seq.empty
      }

    // DropHiveDB command will fail if cascade is false and one or more table exists in database
    if (command.cascade) {
      tablesInDB.foreach { tableName =>
        LeoDropTableCommand(
          DropTableCommand(tableName, ifExists = true, isView = false, purge = true),
          ifExistsSet = true,
          tableName.database,
          tableName.table
        ).run(sparkSession)
      }
    } else if (tablesInDB.nonEmpty) {
      throw new AnalysisException("database is not empty")
    }

    // 4. delete meta in hive metastore
    command.run(sparkSession)

    // 5. drop db in carbon by deleting data folder, and delete bucket if it is using OBS
    try {
      CarbonUtil.dropDatabaseDirectory(databaseLocation)
      LeoEnv.fileSystemType match {
        case FileFactory.FileType.OBS =>
          OBSUtil.deleteBucket(bucket, ifExists, sparkSession)
      }
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        // ignore, we need to delete namespace and meta anyway
        LOGGER.warn(s"failed to delete database folder: $databaseLocation")
    }
    Seq.empty
  }
}
