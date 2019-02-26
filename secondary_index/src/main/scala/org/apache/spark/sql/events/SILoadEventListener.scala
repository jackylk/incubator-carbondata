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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent
import org.apache.carbondata.spark.core.metadata.IndexMetadata

/**
 *
 */
class SILoadEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePreStatusUpdateEvent =>
        LOGGER.info("Load pre status update event-listener called")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePreStatusUpdateEvent]
        val carbonLoadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        val sparkSession = SparkSession.getActiveSession.get
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach {
              indexTableName =>
                val secondaryIndex = SecondaryIndex(Some(carbonTable.getDatabaseName),
                  indexMetadata.getParentTableName,
                  indexMetadata.getIndexesMap.get(indexTableName).asScala.toList,
                  indexTableName)

                CarbonInternalScalaUtil
                  .LoadToSITable(sparkSession,
                    carbonLoadModel,
                    indexTableName,
                    isLoadToFailedSISegments = false,
                    secondaryIndex,
                    carbonTable)
            }
          } else {
            logInfo(s"No index tables found for table: ${carbonTable.getTableName}")
          }
        } else {
          logInfo(s"Index information is null for table: ${carbonTable.getTableName}")
        }
    }
  }
}
