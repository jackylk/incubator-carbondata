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

package org.apache.spark.sql.leo

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.table.{CarbonDescribeFormattedCommand, CarbonShowTablesCommand}
import org.apache.spark.sql.leo.builtin._
import org.apache.spark.sql.leo.command.{LeoCreateDatabaseCommand, LeoCreateTableCommand, LeoDropDatabaseCommand, LeoDropTableCommand}
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory

class LeoDDLStrategy(session: SparkSession) extends SparkStrategy {
  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {

      // DROP DATABASE
      case cmd@CreateDatabaseCommand(_, _, _, _, _) =>
        val leoCmd = LeoCreateDatabaseCommand(cmd)
        ExecutedCommandExec(leoCmd) :: Nil

      // DROP DATABASE
      case cmd@DropDatabaseCommand(_, _, _) =>
        val leoCmd = LeoDropDatabaseCommand(cmd)
        ExecutedCommandExec(leoCmd) :: Nil

      // CREATE TABLE
      case cmd@CreateTableCommand(table, ignoreIfExists) =>
        val leoCmd = LeoCreateTableCommand(table, ignoreIfExists)
        ExecutedCommandExec(leoCmd) :: Nil

      // DROP TABLE
      case cmd@DropTableCommand(identifier, ifNotExists, _, _)
        if CarbonEnv.getInstance(session).carbonMetaStore.isTablePathExists(identifier)(session) =>
        val leoCmd = LeoDropTableCommand(
          cmd, ifNotExists, identifier.database, identifier.table.toLowerCase)
        ExecutedCommandExec(leoCmd) :: Nil

      case ws@WebSearch(_, _) =>
        WebSearchExec(session, ws) :: Nil

      case ei@ExperimentInfo(_, _) =>
        ExperimentInfoExec(session, ei) :: Nil

      case ji@TrainingInfo(_, _) =>
        JobMetricsExec(session, ji) :: Nil

      case _ => Nil
    }
  }
}
