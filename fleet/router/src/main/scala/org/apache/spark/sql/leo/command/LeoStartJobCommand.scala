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
import org.apache.leo.model.rest.ModelRestManager
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.{LeoQueryObject, ModelStoreManager}
import org.apache.spark.sql.{AnalysisException, CarbonEnv, LeoDatabase, Row, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.ObjectSerializationUtil

case class LeoStartJobCommand(
    jobName: String,
    dbName: Option[String],
    modelName: String,
    options: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check if model with modelName already exists
    val modelSchemas = ModelStoreManager.getInstance().getAllModelSchemas
    val updatedDbName =
      LeoDatabase.convertUserDBNameToLeo(CarbonEnv.getDatabaseName(dbName)(sparkSession))
    val updatedModelName = updatedDbName + CarbonCommonConstants.UNDERSCORE + modelName
    val model = modelSchemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(updatedModelName))
    val schema = model.getOrElse(
      throw new AnalysisException("Model with name " + updatedModelName + " already exists in storage"))

    val optionsMap = new java.util.HashMap[String, String]()
    optionsMap.putAll(options.asJava)
    val details = TrainJobManager.getAllTrainedJobs(updatedModelName)
    if (details.exists(_.getJobName.equalsIgnoreCase(jobName))) {
      throw new AnalysisException(
        "Job with name " + jobName + " already exists on model " +updatedModelName)
    }
    val optionsMapFinal = new java.util.HashMap[String, String]()
    optionsMapFinal.putAll(optionsMap)
    optionsMapFinal.putAll(schema.getProperties)
    val str = schema.getProperties.get(CarbonCommonConstants.QUERY_OBJECT)
    val queryObject =
      ObjectSerializationUtil.convertStringToObject(str).asInstanceOf[LeoQueryObject]
    // It starts creating the training job and generates the model in cloud.
    val jobId =
      ModelRestManager.startTrainingJobRequest(optionsMapFinal, updatedModelName, queryObject)
    optionsMap.put("job_id", jobId.toString)
    val detail = new TrainJobDetail(jobName, optionsMap)
    try {
      // store model schema
      TrainJobManager.saveTrainJob(updatedModelName, detail)
    } catch {
      case e:Exception =>
        ModelRestManager.deleteTrainingJob(jobId)
        throw e
    }

    Seq.empty
  }
}
