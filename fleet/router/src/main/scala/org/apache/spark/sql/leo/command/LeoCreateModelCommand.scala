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

import org.apache.leo.model.job.{TrainJobDetail, TrainJobManager}
import org.apache.spark.sql.{AnalysisException, LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}

import org.apache.carbondata.ai.DataScan
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.ObjectSerializationUtil

case class LeoCreateModelCommand(
    modelName: String,
    experimentName: String,
    options: Map[String, String],
    ifNotExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check if model with modelName already exists
    val experimentSchemas = ExperimentStoreManager.getInstance().getAllExperimentSchemas
    val updatedExpName = LeoDatabase.DEFAULT_PROJECTID + CarbonCommonConstants.UNDERSCORE +
                         experimentName
    val experiment = experimentSchemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(updatedExpName))
    val schema = experiment.getOrElse(
      throw new AnalysisException(
        "Experiment with name " + updatedExpName + " does not exist"))

    val optionsMap = new java.util.HashMap[String, String]()
    optionsMap.putAll(options.asJava)
    val details = TrainJobManager.getAllTrainedJobs(updatedExpName)
    if (details.exists(_.getJobName.equalsIgnoreCase(modelName))) {
      if (!ifNotExists) {
        throw new AnalysisException(
          "Model with name " + modelName + " already exists on Experiment " + updatedExpName)
      } else {
        Seq.empty
      }
    }
    val optionsMapFinal = new java.util.HashMap[String, String]()
    optionsMapFinal.putAll(optionsMap)
    optionsMapFinal.putAll(schema.getProperties)
    val str = schema.getProperties.get(CarbonCommonConstants.QUERY_OBJECT)
    val queryObject =
      ObjectSerializationUtil.convertStringToObject(str).asInstanceOf[DataScan]
    // It starts creating the training job and generates the model in cloud.
    val jobId =
      LeoEnv.modelTraingAPI.startTrainingJob(optionsMapFinal, updatedExpName, queryObject)
    optionsMap.put("job_id", jobId.toString)
    val detail = new TrainJobDetail(modelName, optionsMap)
    try {
      // store experiment schema
      TrainJobManager.saveTrainJob(updatedExpName, detail)
    } catch {
      case e: Exception =>
        LeoEnv.modelTraingAPI.stopTrainingJob(jobId)
        throw e
    }

    Seq.empty
  }
}
