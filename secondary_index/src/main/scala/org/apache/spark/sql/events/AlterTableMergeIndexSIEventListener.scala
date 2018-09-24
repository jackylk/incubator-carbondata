/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.events

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events._
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.util.CommonUtil


class AlterTableMergeIndexSIEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val exceptionEvent = event.asInstanceOf[AlterTableMergeIndexEvent]
    val alterTableModel = exceptionEvent.alterTableModel
    val carbonMainTable = exceptionEvent.carbonTable
    val compactionType = alterTableModel.compactionType
    val sparkSession = exceptionEvent.sparkSession
    if (compactionType.equalsIgnoreCase(CompactionType.SEGMENT_INDEX.toString)) {
      LOGGER.audit(s"Compaction request received for table " +
                   s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
      val lock = CarbonLockFactory.getCarbonLockObj(
        carbonMainTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)

      try {
        if (lock.lockWithRetries()) {
          LOGGER.info("Acquired the compaction lock for table" +
                      s" ${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
          val loadFolderDetailsArray = SegmentStatusManager
            .readLoadMetadata(carbonMainTable.getMetadataPath)
          val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
          loadFolderDetailsArray.foreach(loadMetadataDetails => {
            segmentFileNameMap
              .put(loadMetadataDetails.getLoadName,
                String.valueOf(loadMetadataDetails.getLoadStartTime))
          })
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
              val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
                carbonMainTable.getAbsoluteTableIdentifier).asScala
              val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
              validSegments.foreach { segment =>
                validSegmentIds += segment.getSegmentNo
              }
              // Just launch job to merge index for all index tables
              CommonUtil.mergeIndexFiles(
                sparkSession,
                validSegmentIds,
                segmentFileNameMap,
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