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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{DropTableCommand, RunnableCommand}
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand
import org.apache.spark.sql.leo.LeoEnv

import org.apache.carbondata.core.datastore.impl.FileFactory

case class LeoDropTableCommand(
    sparkCommand: DropTableCommand,
    ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    dropChildTable: Boolean = false)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check whether the table exists
    if (!CarbonEnv.isTableExists(databaseNameOp, tableName)(sparkSession)) {
      if (ifExistsSet) return Seq.empty
      else throw new AnalysisException("table " +
                                       CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession) +
                                       "." + tableName + " is not exist")
    }

    // delete carbon table
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    CarbonDropTableCommand(ifExistsSet = ifExistsSet, databaseNameOp, tableName).run(sparkSession)

    // delete the carbon table folder
    FileFactory.getCarbonFile(carbonTable.getTablePath).delete()
    Seq.empty
  }
}
