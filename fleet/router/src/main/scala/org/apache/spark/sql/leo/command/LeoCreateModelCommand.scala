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

import com.huawei.cloud.modelarts.DataScan
import org.apache.spark.sql.{AnalysisException, LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.ObjectSerializationUtil
import org.apache.carbondata.leo.job.modelarts.{TrainModelDetail, TrainModelManager}

case class LeoCreateModelCommand(
    modelName: String,
    experimentName: String,
    options: Map[String, String],
    ifNotExists: Boolean)
  extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

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
    val details = TrainModelManager.getAllEnabledTrainedModels(updatedExpName)
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
      LeoEnv.modelTraingAPI.startTrainingJob(optionsMapFinal,
        updatedExpName + CarbonCommonConstants.UNDERSCORE + modelName, queryObject)
    optionsMap.put("job_id", jobId.toString)
    val detail = new TrainModelDetail(modelName, optionsMap)
    try {
      // store experiment schema
      TrainModelManager.saveTrainModel(updatedExpName, detail)
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        LeoEnv.modelTraingAPI.stopTrainingJob(jobId)
        throw e
    }

    Seq.empty
  }
}
