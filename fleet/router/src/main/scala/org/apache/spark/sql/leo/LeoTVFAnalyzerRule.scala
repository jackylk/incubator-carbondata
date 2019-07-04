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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.leo.builtin._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class LeoTVFAnalyzerRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case p: Project if p.child.isInstanceOf[UnresolvedTableValuedFunction] =>
        val tvf = p.child.asInstanceOf[UnresolvedTableValuedFunction]
        LeoTVFunctions.withName(tvf.functionName.toLowerCase) match {
          case LeoTVFunctions.websearch =>
            val output: Seq[Attribute] =
              StructType(StructField("url", StringType, nullable = false) ::
                         StructField("title", StringType, nullable = false) :: Nil).toAttributes
            p.copy(child = WebSearch(output,
              new WebSearchParams(tvf.functionArgs)))
          case LeoTVFunctions.experiment_info =>
            val output: Seq[Attribute] =
              StructType(StructField("JobName", StringType, nullable = false) ::
                         StructField("JobProperties", StringType, nullable = false) ::
                         StructField("Status", StringType, nullable = false) :: Nil).toAttributes
            p.copy(child = ExperimentInfo(output, new ExperimentInfoParams(tvf.functionArgs)))
          case LeoTVFunctions.training_info =>
            val output: Seq[Attribute] =
              StructType(StructField("Job_Id", StringType, nullable = false) ::
                         StructField("Job_Name", StringType, nullable = false) ::
                         StructField("Status", StringType, nullable = false) ::
                         StructField("Duration", StringType, nullable = false) :: Nil).toAttributes
            p.copy(child = TrainingInfo(output, new TrainingInfoParams(tvf.functionArgs)))
          case LeoTVFunctions.model_info =>
            val output: Seq[Attribute] =
              StructType(StructField("Model_Id", StringType, nullable = false) ::
                         StructField("Model_Name", StringType, nullable = false) ::
                         StructField("Model_Type", StringType, nullable = false) ::
                         StructField("Model_Size", StringType, nullable = false) ::
                         StructField("Model_Status", StringType, nullable = false) ::
                         StructField("Model_Version", StringType, nullable = false) :: Nil)
                .toAttributes
            p.copy(child = ModelInfo(output, new ModelInfoParams(tvf.functionArgs)))
          case _ =>
            p
        }
      case other => other
    }
  }

  object LeoTVFunctions extends Enumeration {
    type leoTVFunctions = Value
    val websearch, experiment_info, training_info, model_info = Value
  }

}

