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

import com.huawei.cloud.modelarts.ModelArtsModelAPI
import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.apache.spark.sql.leo.builtin.{LeoUDF, ModelArtsUdf}

import org.apache.carbondata.ai.ModelAPI

object LeoEnv {
  def getOrCreateLeoSession(builder: SparkSession.Builder): SparkSession = {
    builder
      .config("leo.enabled", "true")
      .config("spark.carbon.sessionstate.classname",
        "org.apache.spark.sql.leo.LeoSessionStateBuilder")
      .enableHiveSupport()

    val session = new CarbonSession.CarbonBuilder(builder).getOrCreateCarbonSession()
    // register all ModelArts udf to spark
//    ModelArtsUdf.registerAllModelArtsUDF(session)
    registerLeoBuiltinUDF(session)
  }

  private def registerLeoBuiltinUDF(sesssion: SparkSession): SparkSession = {
    val download: String => Array[Byte] = LeoUDF.download
    sesssion.udf.register("download", download)
    sesssion
  }

  private lazy val modelTrainingAPIInstance = new ModelArtsModelAPI

  def modelTraingAPI: ModelAPI = modelTrainingAPIInstance
}
