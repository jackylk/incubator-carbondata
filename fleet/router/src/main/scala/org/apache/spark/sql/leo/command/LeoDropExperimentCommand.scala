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

import org.apache.spark.sql.{LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}
import org.apache.spark.sql.leo.exceptions.NoSuchExperimentException

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.leo.job.modelarts.TrainModelManager

case class LeoDropExperimentCommand(
    experimentName: String,
    ifExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val experimentSchemas = ExperimentStoreManager.getInstance().getAllExperimentSchemas
    val updatedExpName = LeoDatabase.DEFAULT_PROJECTID + CarbonCommonConstants.UNDERSCORE +
                         experimentName
    val ifExperimentExists = experimentSchemas.asScala
      .exists(experiment => experiment.getDataMapName.equalsIgnoreCase(updatedExpName))
    if (ifExperimentExists) {
      val details = TrainModelManager.getAllEnabledTrainedModels(updatedExpName)
      details.foreach { d =>
        val jobId = d.getProperties.get("job_id")
        LeoEnv.modelTraingAPI.stopTrainingJob(jobId.toLong)
      }
      TrainModelManager.dropModel(updatedExpName)
      ExperimentStoreManager.getInstance().dropExperimentSchema(updatedExpName)
    } else {
      if (!ifExists) {
        throw new NoSuchExperimentException(updatedExpName)
      }
    }
    Seq.empty
  }
}
