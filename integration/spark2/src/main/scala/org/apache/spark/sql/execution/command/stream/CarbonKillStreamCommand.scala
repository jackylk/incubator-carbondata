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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.MetadataCommand

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.stream.StreamJobManager

/**
 * Stop the stream for specified sink table
 */
case class CarbonKillStreamCommand(
    dbName: Option[String],
    tableName: String
) extends MetadataCommand {
  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(dbName, tableName)(sparkSession)
    val jobIdOp = StreamJobManager.getJobIdOnTable(carbonTable)
    if (jobIdOp.isEmpty) {
      throw new MetadataProcessException(
        s"table not found: ${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
    }
    StreamJobManager.killJob(jobIdOp.get)
    Seq.empty
  }
}
