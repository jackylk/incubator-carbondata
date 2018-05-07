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
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CarbonInternalCommonUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{AlterTableCompactionPostEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

class MergeIndexEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePostExecutionEvent =>
        LOGGER.audit("Load post status event-listener called for merge index")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePostExecutionEvent]
        val carbonTableIdentifier = loadTablePreStatusUpdateEvent.getCarbonTableIdentifier
        val loadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = loadModel.getCarbonDataLoadSchema.getCarbonTable
        val compactedSegments = loadModel.getMergedSegmentIds
        val sparkSession = SparkSession.getActiveSession.get
        if (null != compactedSegments && !compactedSegments.isEmpty) {
          mergeIndexFilesForCompactedSegments(sparkSession.sparkContext,
            carbonTable,
            compactedSegments)
        } else {
        CarbonInternalCommonUtil.mergeIndexFiles(sparkSession.sparkContext,
          Seq(loadModel.getSegmentId),
          carbonTable.getTablePath,
          carbonTable, false)
    }
      case alterTableCompactionPostEvent: AlterTableCompactionPostEvent =>
        LOGGER.audit("Merge index for compaction called")
        val alterTableCompactionPostEvent = event.asInstanceOf[AlterTableCompactionPostEvent]
        val carbonTable = alterTableCompactionPostEvent.carbonTable
        val mergedLoads = alterTableCompactionPostEvent.compactedLoads
        val sparkContext = alterTableCompactionPostEvent.sparkSession.sparkContext
        mergeIndexFilesForCompactedSegments(sparkContext, carbonTable, mergedLoads)
  }
  }

  def mergeIndexFilesForCompactedSegments(sparkContext: SparkContext,
    carbonTable: CarbonTable,
    mergedLoads: util.List[String]): Unit = {
    // get only the valid segments of the table
    val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
      carbonTable.getAbsoluteTableIdentifier).asScala
    val mergedSegmentIds = new util.ArrayList[String]()
    mergedLoads.asScala.foreach(mergedLoad => {
      val loadName = mergedLoad
        .substring(mergedLoad.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                   CarbonCommonConstants.LOAD_FOLDER.length)
      mergedSegmentIds.add(loadName)
    })
    // filter out only the valid segments from the list of compacted segments
    // Example: say compacted segments list contains 0.1, 3.1, 6.1, 0.2.
    // In this list 0.1, 3.1 and 6.1 are compacted to 0.2 in the level 2 compaction.
    // So, it is enough to do merge index only for 0.2 as it is the only valid segment in this list
    val validMergedSegIds = validSegments
      .filter { seg => mergedSegmentIds.contains(seg.getSegmentNo) }.map(_.getSegmentNo)
    if (null != validMergedSegIds && !mergedSegmentIds.isEmpty) {
      CarbonInternalCommonUtil
        .mergeIndexFiles(sparkContext,
          validMergedSegIds,
          carbonTable.getTablePath,
          carbonTable,
          false)
    }
  }
}
