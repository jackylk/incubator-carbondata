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

import scala.collection.JavaConverters._

import org.apache.leo.model.job.TrainModelManager
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}
import org.apache.spark.sql.leo.exceptions.NoSuchExperimentException


case class LeoDropExperimentCommand(
    experimentName: String,
    ifExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val experimentSchemas = ExperimentStoreManager.getInstance().getAllExperimentSchemas
    val ifExperimentExists = experimentSchemas.asScala
      .exists(experiment => experiment.getDataMapName.equalsIgnoreCase(experimentName))
    if (ifExperimentExists) {
      val details = TrainModelManager.getAllEnabledTrainedModels(experimentName)
      details.foreach { d =>
        val jobId = d.getProperties.get("job_id")
        LeoEnv.modelTraingAPI.stopTrainingJob(jobId.toLong)
      }
      TrainModelManager.dropModel(experimentName)
      ExperimentStoreManager.getInstance().dropExperimentSchema(experimentName)
    } else {
      if (!ifExists) {
        throw new NoSuchExperimentException(experimentName)
      }
    }
    Seq.empty
  }
}
