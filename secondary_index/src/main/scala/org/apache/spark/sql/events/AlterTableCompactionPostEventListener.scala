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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.util.Compactor

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.events.{AlterTableCompactionPreStatusUpdateEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class AlterTableCompactionPostEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableCompactionPostEvent: AlterTableCompactionPreStatusUpdateEvent =>
        LOGGER.audit("post load event-listener called")
        val carbonLoadModel = alterTableCompactionPostEvent.carbonLoadModel
        val sQLContext = alterTableCompactionPostEvent.sparkSession.sqlContext
        val mergedLoadName = alterTableCompactionPostEvent.mergedLoadName
        val loadMetadataDetails = new LoadMetadataDetails
        loadMetadataDetails.setLoadName(mergedLoadName)
        val loadsToMerge: util.List[LoadMetadataDetails] = List(loadMetadataDetails).asJava
        val loadName = mergedLoadName
          .substring(mergedLoadName.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                     CarbonCommonConstants.LOAD_FOLDER.length)
        val mergeLoadStartTime = CarbonUpdateUtil.readCurrentTime()

        val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
          scala
            .collection.mutable.Map((loadName, mergeLoadStartTime))
        Compactor.createSecondaryIndexAfterCompaction(sQLContext,
          carbonLoadModel,
          List(loadName),
          loadsToMerge,
          segmentIdToLoadStartTimeMapping, true)
      case _ =>
    }
  }
}
