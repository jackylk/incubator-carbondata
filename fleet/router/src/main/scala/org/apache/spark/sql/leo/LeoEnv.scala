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

import java.nio.file.Paths

import com.huawei.cloud.modelarts.ModelArtsModelAPI
import org.apache.spark.sql.{CarbonEnv, CarbonSession, SQLContext, SparkSession}
import org.apache.spark.sql.leo.builtin.LeoUDF
import org.apache.spark.sql.leo.builtin.LeoUDF
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.carbondata.cloud.CloudUdfRegister
import org.apache.carbondata.ai.ModelAPI
import org.apache.carbondata.core.constants.CarbonCommonConstants

import scala.reflect.io.File

object LeoEnv {
  val fileSystemType = FileType.OBS

  def getOrCreateLeoSession(builder: SparkSession.Builder): SparkSession = {
    builder
      .config("leo.enabled", "true")
      .config("spark.carbon.sessionstate.classname",
        "org.apache.spark.sql.leo.LeoSessionStateBuilder")
      .enableHiveSupport()

    val session = new CarbonSession.CarbonBuilder(builder).getOrCreateCarbonSession()
    registerUDFs(session)
    session
  }

  def bucketName(dbName: String): String = {
    "leo-db-" + dbName
  }

  def getDefaultDBPath(dbName: String, sparkSession: SparkSession): String = {
    fileSystemType match {
      case FileFactory.FileType.OBS =>
        CarbonCommonConstants.OBS_PREFIX + bucketName(dbName) + "/" + dbName + ".db"
      case _ =>
        try {
          CarbonEnv.getDatabaseLocation(dbName, sparkSession)
        } catch {
          case e: NoSuchDatabaseException =>
            CarbonProperties.getStorePath
        }

    }
  }

  private def registerLeoBuiltinUDF(sesssion: SparkSession): SparkSession = {
    val download: String => Array[Byte] = LeoUDF.download
    sesssion.udf.register("download", download)
    sesssion
  }

  private def registerVideoUDFs(session: SparkSession): Unit = {
    val scriptsDirPath = System.getProperty("user.dir") + "/fleet/vision/src/main/scala/org/apache/spark/sql/leo/video/"
    VisionSparkUDFs.registerExtractFramesFromVideo(session, scriptsDirPath)
  }

  def registerUDFs(session: SparkSession): Unit = {
    CloudUdfRegister.register(session)
    registerLeoBuiltinUDF(session)
    VisionSparkUDFs.registerAll(session.sqlContext)
    registerVideoUDFs(session)
  }


  private lazy val modelTrainingAPIInstance = new ModelArtsModelAPI

  def modelTraingAPI: ModelAPI = modelTrainingAPIInstance
}
