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

import org.apache.leo.model.job.TrainJobManager
import org.apache.spark.sql.{AnalysisException, LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Drop's model on given experimentName
 */
case class LeoDropModelCommand(
    modelName: String,
    experimentName: String,
    ifExists: Boolean
) extends RunnableCommand{

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check if model with modelName already exists
    val modelSchemas =  ExperimentStoreManager.getInstance().getAllExperimentSchemas
    val updatedExpName = LeoDatabase.DEFAULT_PROJECTID + CarbonCommonConstants.UNDERSCORE +
                         experimentName
    val model = modelSchemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(updatedExpName))
    val schema = model.getOrElse(
      throw new AnalysisException(
        "Experiment with name " + experimentName + " doesn't exists in storage"))

    val details = TrainJobManager.getAllTrainedJobs(updatedExpName)
    val jobDetail = details.find(_.getJobName.equalsIgnoreCase(modelName)).
      getOrElse(throw new AnalysisException(
      "Model with name " + modelName + " doesn't exists on experiment " + experimentName))
    val jobId = jobDetail.getProperties.get("job_id")
    LeoEnv.modelTraingAPI.stopTrainingJob(jobId.toLong)
    TrainJobManager.dropTrainJob(updatedExpName, jobDetail.getJobName)
    Seq.empty
  }
}
