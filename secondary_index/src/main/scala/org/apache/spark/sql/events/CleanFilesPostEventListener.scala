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

package org.apache.spark.sql.events

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.events.{CleanFilesPostEvent, Event, OperationContext,
OperationEventListener}

/**
 *
 */
class CleanFilesPostEventListener extends OperationEventListener with Logging {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case cleanFilesPostEvent: CleanFilesPostEvent =>
        val carbonTable = cleanFilesPostEvent.carbonTable
        val sparkSession = cleanFilesPostEvent.sparkSession
        CarbonInternalScalaUtil.getIndexesTables(carbonTable).asScala.foreach { tableName =>
          val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
          val table = metastore
            .lookupRelation(Some(carbonTable.getDatabaseName), tableName)(sparkSession)
            .asInstanceOf[CarbonRelation].carbonTable

          FileInternalUtil.cleanIndexFiles(table, table.getTablePath, true)
        }
    }
  }
}
