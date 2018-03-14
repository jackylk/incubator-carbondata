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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CarbonInternalCommonUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent

class MergeIndexEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePreStatusUpdateEvent =>
        LOGGER.audit("Load pre status update event-listener called for merge index")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePreStatusUpdateEvent]
        val carbonTableIdentifier = loadTablePreStatusUpdateEvent.getCarbonTableIdentifier
        val loadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = loadModel.getCarbonDataLoadSchema.getCarbonTable
        val sparkSession = SparkSession.getActiveSession.get
        CarbonInternalCommonUtil.mergeIndexFiles(sparkSession.sparkContext,
          Seq(loadModel.getSegmentId),
          carbonTable.getTablePath,
          carbonTable, false)
    }
  }
}
