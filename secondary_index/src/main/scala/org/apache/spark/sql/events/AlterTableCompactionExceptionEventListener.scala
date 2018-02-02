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
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.{CarbonInternalCommonUtil, CarbonInternalScalaUtil}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
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
          CarbonInternalCommonUtil.mergeIndexFiles(sparkSession.sparkContext,
            CarbonDataMergerUtil.getValidSegmentList(
              carbonMainTable.getAbsoluteTableIdentifier).asScala,
            carbonMainTable.getTablePath,
            carbonMainTable,
            true)
          val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
          if (null != indexTablesList && indexTablesList.nonEmpty) {
            indexTablesList.foreach { indexTableAndColumns =>
              val secondaryIndex = SecondaryIndex(Some(carbonMainTable.getDatabaseName),
                carbonMainTable.getTableName,
                indexTableAndColumns._2.asScala.toList,
                indexTableAndColumns._1)
              val metastore = CarbonEnv.getInstance(sparkSession)
                .carbonMetastore
              val indexCarbonTable = metastore
                .lookupRelation(Some(carbonMainTable.getDatabaseName),
                  secondaryIndex.indexTableName)(sparkSession).asInstanceOf[CarbonRelation]
                .carbonTable

              // Just launch job to merge index for all index tables
              CarbonInternalCommonUtil.mergeIndexFiles(
                sparkSession.sparkContext,
                CarbonDataMergerUtil.getValidSegmentList(
                  indexCarbonTable.getAbsoluteTableIdentifier).asScala,
                indexCarbonTable.getTablePath,
                indexCarbonTable,
                true)
            }
          }
          LOGGER.audit(s"Compaction request completed for table " +
                       s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          LOGGER.info(s"Compaction request completed for table " +
                      s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
        } else {
          LOGGER.audit("Not able to acquire the compaction lock for table " +
                       s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          LOGGER.error(s"Not able to acquire the compaction lock for table" +
                       s" ${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
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
