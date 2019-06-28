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

  private val leoTVFunctions: Seq[String] =
    Seq("WebSearch")

  private val leoExperimentTVFunctions: Seq[String] =
    Seq("Experiment_Info")

  private val leoTrainingTVFunctions: Seq[String] =
    Seq("Training_Info")

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case p: Project if p.child.isInstanceOf[UnresolvedTableValuedFunction] =>
        val tvf = p.child.asInstanceOf[UnresolvedTableValuedFunction]
        if (leoTVFunctions.contains(tvf.functionName)) {
          val output: Seq[Attribute] =
            StructType(StructField("url", StringType, nullable = false) ::
                       StructField("title", StringType, nullable = false) :: Nil).toAttributes
          p.copy(child = WebSearch(output,
            new WebSearchParams(tvf.functionArgs)))
        } else if (leoExperimentTVFunctions.exists(f => f.equalsIgnoreCase(tvf.functionName))) {
          val output: Seq[Attribute] =
            StructType(StructField("JobName", StringType, nullable = false) ::
                       StructField("JobProperties", StringType, nullable = false) ::
                       StructField("Status", StringType, nullable = false) :: Nil).toAttributes
          p.copy(child = ExperimentInfo(output, new ExperimentInfoParams(tvf.functionArgs)))
        } else if (leoTrainingTVFunctions.exists(f => f.equalsIgnoreCase(tvf.functionName))) {
          val output: Seq[Attribute] =
            StructType(StructField("Job_Id", StringType, nullable = false) ::
                       StructField("Job_Name", StringType, nullable = false) ::
                       StructField("Status", StringType, nullable = false) ::
                       StructField("Duration", StringType, nullable = false) :: Nil).toAttributes
          p.copy(child = TrainingInfo(output, new TrainingInfoParams(tvf.functionArgs)))
        } else {
          p
        }
      case other => other
    }
  }

}

