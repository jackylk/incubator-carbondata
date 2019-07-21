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
import scala.util.control.Breaks

import org.apache.leo.model.job.TrainModelManager
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.leo.builtin.MoldelArtsUdf
import org.apache.spark.sql.leo.{ExperimentStoreManager, LeoEnv}

case class LeoRegisterModelCommand(
    experimentName: String,
    modelName: String,
    udfName: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    // check if model with modelName already exists
    val experimentSChemas = ExperimentStoreManager.getInstance().getAllExperimentSchemas
    val experiment = experimentSChemas.asScala
      .find(model => model.getDataMapName.equalsIgnoreCase(experimentName))
    val schema = experiment.getOrElse(
      throw new AnalysisException(
        "Experiment with name " + experimentName + " doesn't exists in storage"))
    val details = TrainModelManager.getAllTrainedModels(experimentName)
    val jobDetail = details.find(_.getJobName.equalsIgnoreCase(modelName)).
      getOrElse(throw new AnalysisException(
        "Model with name " + modelName + " doesn't exists on experiment " + experimentName))
    val modelId = LeoEnv.modelTraingAPI
      .importModel(jobDetail.getProperties, udfName)
    jobDetail.getProperties.put("model_id", modelId)
    var loop = new Breaks
    loop.breakable {
      while (true) {
        // wait till status changes to published
        Thread.sleep(10000)
        val map = LeoEnv.modelTraingAPI.getModelDetails(modelId)
        val status = map.get("model_status")
        if (status != null && status.toString.equalsIgnoreCase("published")) {
          loop.break()
        }
      }
    }
    jobDetail.getProperties.put("udfName", udfName)
    Thread.sleep(10000)
    val serviceId = LeoEnv.modelTraingAPI
      .deployModel(jobDetail.getProperties, udfName)
    loop = new Breaks
    loop.breakable {
      while (true) {
        //wait till it is deployed and gets the address.
        Thread.sleep(10000)
        val map = LeoEnv.modelTraingAPI.getModelServiceDetails(serviceId)
        if (map.get("status").equals("running")) {
          jobDetail.getProperties.put("access_address", map.get("access_address").toString)
          loop.break()
        }
      }
    }
    jobDetail.getProperties.put("service_id", serviceId)
    TrainModelManager.updateTrainModel(experimentName, jobDetail)
    MoldelArtsUdf.register(sparkSession, jobDetail, udfName)
    Seq.empty
  }
}
