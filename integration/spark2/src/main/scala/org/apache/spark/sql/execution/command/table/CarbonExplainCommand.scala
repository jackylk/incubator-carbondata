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

import org.apache.spark.sql.{AnalysisException, CarbonEnv, LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericRow}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Union}
import org.apache.spark.sql.execution.command.{ExplainCommand, MetadataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.profiler.ExplainCollector

case class CarbonExplainCommand(
    child: LogicalPlan,
    override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())
) extends MetadataCommand {
  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val explainCommand = child.asInstanceOf[ExplainCommand]
    setAuditInfo(Map("query" -> explainCommand.logicalPlan.simpleString))
    val (newPlanOp, errMsg) = LeoDatabase.convertUserDBNameToLeoInPlan(explainCommand.logicalPlan)
    val newCmd = if (newPlanOp.isEmpty) {
      throw new AnalysisException(errMsg)
    } else {
      ExplainCommand(
        newPlanOp.get,
        explainCommand.extended,
        explainCommand.codegen,
        explainCommand.cost)
    }
    val isCommand = newCmd.logicalPlan match {
      case _: Command => true
      case Union(childern) if childern.forall(_.isInstanceOf[Command]) => true
      case _ => false
    }

    val rows = if (newCmd.logicalPlan.isStreaming || isCommand) {
      newCmd.run(sparkSession)
    } else {
      collectProfiler(sparkSession) ++ newCmd.run(sparkSession)
    }
    val env = CarbonEnv.getInstance(sparkSession)
    rows.map { row =>
      val newMsg = env.makeStringValidToUser(row.getString(0))
      new GenericRow(Seq(newMsg).toArray.asInstanceOf[Array[Any]])
    }
  }

  private def collectProfiler(sparkSession: SparkSession): Seq[Row] = {
    try {
      ExplainCollector.setup()
      if (ExplainCollector.enabled()) {
        val queryExecution =
          sparkSession.sessionState.executePlan(child.asInstanceOf[ExplainCommand].logicalPlan)
        queryExecution.toRdd.partitions
        // For count(*) queries the explain collector will be disabled, so profiler
        // informations not required in such scenarios.
        if (null == ExplainCollector.getFormatedOutput) {
          Seq.empty
        }
        Seq(Row("== CarbonData Profiler ==\n" + ExplainCollector.getFormatedOutput))
      } else {
        Seq.empty
      }
    } finally {
      ExplainCollector.remove()
    }
  }

  override protected def opName: String = "EXPLAIN"
}

