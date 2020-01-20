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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.DatabaseLocationProvider
import org.apache.carbondata.core.util.CarbonProperties

/**
 * environment related code
 */
object EnvHelper {

  def isLuxor(sparkSession: SparkSession): Boolean = {
    sparkSession.sqlContext.conf.luxorEnabled
  }

  def isLeo(sparkSession: SparkSession): Boolean = {
    CarbonEnv.getInstance(sparkSession).isLeo
  }

  def isPrivacy(sparkSession: SparkSession, isExternal: Boolean): Boolean = {
    (!isExternal) && (isLeo(sparkSession) || isLuxor(sparkSession))
  }

  def setDefaultHeader(
      sparkSession: SparkSession,
      optionsFinal: java.util.Map[String, String]
  ): Unit = {
    if (isLuxor(sparkSession)) {
      val fileHeader = optionsFinal.get("fileheader")
      val header = optionsFinal.get("header")
      if (StringUtils.isEmpty(fileHeader) && StringUtils.isEmpty(header)) {
        optionsFinal.put("header", "false")
      }
    }
  }

  def isRetainData(sparkSession: SparkSession, retainData: Boolean): Boolean = {
    if (isLuxor(sparkSession)) {
      retainData
    } else {
      true
    }
  }

  def getDatabase(database: String): String = {
    DatabaseLocationProvider.get().provide(database)
  }

  // During the init process of CarbonEnv check the Environment is DLI or not
  // At DLI Environment:set the CARBON_DATAMAP_SCHEMA_STORAGE="DATABASE"
  // Else:This property is null and will be set as CARBON_DATAMAP_SCHEMA_STORAGE_DATABASE="DISK" after init
  def setDataMapLocation(sparkSession: SparkSession): Unit = {
    if (isLuxor(sparkSession)) {
      CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE,
        CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE_DATABASE)
    }
  }
}
