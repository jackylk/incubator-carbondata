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
import org.apache.spark.util.CarbonInternalCommonUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.events.{AlterTableCompactionPreStatusUpdateEvent, Event, OperationContext, OperationEventListener}

class MergeIndexCompactionEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    LOGGER.audit("Merge index for compaction called")
    val loadCompactionEvent = event.asInstanceOf[AlterTableCompactionPreStatusUpdateEvent]
    val carbonTable = loadCompactionEvent.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val mergedLoadName = loadCompactionEvent.mergedLoadName
    val loadName = mergedLoadName
      .substring(mergedLoadName.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                 CarbonCommonConstants.LOAD_FOLDER.length)
    val sparkContext = loadCompactionEvent.sparkSession.sparkContext
    val tablePath = carbonTable.getTablePath
    CarbonInternalCommonUtil
      .mergeIndexFiles(sparkContext, Seq(loadName), tablePath, carbonTable, false)
  }
}
