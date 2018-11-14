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
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CarbonInternalScalaUtil, Compactor}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.events.{AlterTableCompactionPreStatusUpdateEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.util.CommonUtil


/**
 *
 */
class AlterTableCompactionPostEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableCompactionPostEvent: AlterTableCompactionPreStatusUpdateEvent =>
        LOGGER.info("post load event-listener called")
        val carbonLoadModel = alterTableCompactionPostEvent.carbonLoadModel
        val sQLContext = alterTableCompactionPostEvent.sparkSession.sqlContext
        val compactionType: CompactionType = alterTableCompactionPostEvent.carbonMergerMapping
          .campactionType
        if (compactionType.toString
          .equalsIgnoreCase(CompactionType.SEGMENT_INDEX.toString)) {
          val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
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
              val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
                carbonLoadModel.getTableName,
                indexTableAndColumns._2.asScala.toList,
                indexTableAndColumns._1)
              val metastore = CarbonEnv.getInstance(sQLContext.sparkSession)
                .carbonMetastore
              val indexCarbonTable = metastore
                .lookupRelation(Some(carbonLoadModel.getDatabaseName),
                  secondaryIndex.indexTableName)(sQLContext
                  .sparkSession).asInstanceOf[CarbonRelation].carbonTable

              val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
                carbonMainTable.getAbsoluteTableIdentifier).asScala
              val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
              validSegments.foreach { segment =>
                validSegmentIds += segment.getSegmentNo
              }
              // Just launch job to merge index for all index tables
              CarbonMergeFilesRDD.mergeIndexFiles(
                sQLContext.sparkSession,
                validSegmentIds,
                segmentFileNameMap,
                indexCarbonTable.getTablePath,
                indexCarbonTable,
                true)
            }
          }
        } else {
          val mergedLoadName = alterTableCompactionPostEvent.mergedLoadName
          val loadMetadataDetails = new LoadMetadataDetails
          loadMetadataDetails.setLoadName(mergedLoadName)
          val validSegments: Array[Segment] = alterTableCompactionPostEvent.carbonMergerMapping
            .validSegments
          val loadsToMerge: mutable.Buffer[String] = mutable.Buffer[String]()
          validSegments.foreach { segment =>
            loadsToMerge += segment.getSegmentNo
          }
          val loadName = mergedLoadName
            .substring(mergedLoadName.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                       CarbonCommonConstants.LOAD_FOLDER.length)
          val mergeLoadStartTime = CarbonUpdateUtil.readCurrentTime()

          val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang
          .Long] = scala.collection.mutable.Map((loadName, mergeLoadStartTime))
          Compactor.createSecondaryIndexAfterCompaction(sQLContext,
            carbonLoadModel,
            List(loadName),
            loadsToMerge.toArray,
            segmentIdToLoadStartTimeMapping, true)
        }
      case _ =>
    }
  }
}
