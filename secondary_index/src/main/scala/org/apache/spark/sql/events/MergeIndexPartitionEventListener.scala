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

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTableMergePartitionEvent
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants

class MergeIndexPartitionEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    LOGGER.audit("Merge index for partition called")
    val loadPartitionEvent = event.asInstanceOf[LoadTableMergePartitionEvent]
    val segmentPath = loadPartitionEvent.getSegmentPath
    var mergeIndex: Boolean = false
    try
      mergeIndex = (CarbonProperties.getInstance
        .getProperty(CarbonInternalCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonInternalCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)).toBoolean

    catch {
      case e: Exception =>
        mergeIndex = CarbonInternalCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT.toBoolean
    }
    if (mergeIndex) {
      //TODO. temporary change done to pass Null as this class will be rewritten for merge index support.
      new CarbonIndexFileMergeWriter().mergeCarbonIndexFilesOfSegment(segmentPath, null)
    }
  }
}
