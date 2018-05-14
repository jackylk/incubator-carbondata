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
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonInternalCommonUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{AlterTableCompactionExceptionEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, InternalCompactionType}

class AlterTableCompactionExceptionEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val exceptionEvent = event.asInstanceOf[AlterTableCompactionExceptionEvent]
    val alterTableModel = exceptionEvent.alterTableModel
    val carbonMainTable = exceptionEvent.carbonTable
    val compactionType = alterTableModel.compactionType
    val sparkSession = exceptionEvent.sparkSession
    if (compactionType.equalsIgnoreCase(InternalCompactionType.SEGMENT_INDEX.toString)) {
      LOGGER.audit(s"Compaction request received for table " +
                   s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
      val lock = CarbonLockFactory.getCarbonLockObj(
        carbonMainTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)

      try {
        if (lock.lockWithRetries()) {
          LOGGER.info("Acquired the compaction lock for table" +
                      s" ${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
            carbonMainTable.getAbsoluteTableIdentifier).asScala
          val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
          validSegments.foreach { segment =>
            validSegmentIds += segment.getSegmentNo
          }
          val loadFolderDetailsArray = SegmentStatusManager
            .readLoadMetadata(carbonMainTable.getMetadataPath)
          val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
          loadFolderDetailsArray.foreach(loadMetadataDetails => {
            segmentFileNameMap
              .put(loadMetadataDetails.getLoadName, loadMetadataDetails.getSegmentFile)
          })
          CarbonInternalCommonUtil.mergeIndexFiles(sparkSession.sparkContext,
            validSegmentIds,
            segmentFileNameMap,
            carbonMainTable.getTablePath,
            carbonMainTable,
            true)
          val requestMessage = "Compaction request completed for table "
                               s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}"
          LOGGER.audit(requestMessage)
          LOGGER.info(requestMessage)
        } else {
          val lockMessage = "Not able to acquire the compaction lock for table " +
                            s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}"
          LOGGER.audit(lockMessage)
          LOGGER.error(lockMessage)
          CarbonException.analysisException(
            "Table is already locked for compaction. Please try after some time.")
        }
      } finally {
        lock.unlock()
      }
      operationContext.setProperty("compactionException", "false")
    }
  }
}