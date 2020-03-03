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

package org.apache.spark.sql

import org.apache.commons.lang3.StringUtils

import org.apache.carbondata.core.metadata.DatabaseLocationProvider

/**
 * environment related code
 */
object EnvHelper {

  def isCloud(sparkSession: SparkSession): Boolean = {
    val conf = sparkSession.sqlContext.getConf("spark.sql.luxor.enabled", "false")
    conf != null && conf.toBoolean
  }

  def isPrivacy(sparkSession: SparkSession, isExternal: Boolean): Boolean = {
    (!isExternal) && isCloud(sparkSession)
  }

  def setDefaultHeader(
      sparkSession: SparkSession,
      optionsFinal: java.util.Map[String, String]
  ): Unit = {
    if (isCloud(sparkSession)) {
      val fileHeader = optionsFinal.get("fileheader")
      val header = optionsFinal.get("header")
      if (StringUtils.isEmpty(fileHeader) && StringUtils.isEmpty(header)) {
        optionsFinal.put("header", "false")
      }
    }
  }

  def isRetainData(sparkSession: SparkSession, retainData: Boolean): Boolean = {
    if (isCloud(sparkSession)) {
      retainData
    } else {
      true
    }
  }

  def getDatabase(database: String): String = {
    DatabaseLocationProvider.get().provide(database)
  }
}
