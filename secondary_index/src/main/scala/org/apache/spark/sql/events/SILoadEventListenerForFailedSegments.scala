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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostStatusUpdateEvent
import org.apache.carbondata.spark.core.metadata.IndexMetadata

/**
 * This Listener is to load the data to failed segments of Secondary index table(s)
 */
class SILoadEventListenerForFailedSegments extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case postStatusUpdateEvent: LoadTablePostStatusUpdateEvent =>
        LOGGER.info("Load post status update event-listener called")
        val loadTablePostStatusUpdateEvent = event.asInstanceOf[LoadTablePostStatusUpdateEvent]
        val carbonLoadModel = loadTablePostStatusUpdateEvent.getCarbonLoadModel
        val sparkSession = SparkSession.getActiveSession.get
        // when Si creation and load to main table are parallel, get the carbonTable from the
        // metastore which will have the latest index Info
        val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        val carbonTable = metaStore
          .lookupRelation(Some(carbonLoadModel.getDatabaseName),
            carbonLoadModel.getTableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach {
              indexTableName =>
                val isLoadSIForFailedSegments = sparkSession.sessionState.catalog
                  .getTableMetadata(TableIdentifier(indexTableName,
                    Some(carbonLoadModel.getDatabaseName))).storage.properties
                  .getOrElse("isSITableEnabled", "true").toBoolean

                if (!isLoadSIForFailedSegments) {
                  val secondaryIndex = SecondaryIndex(Some(carbonTable.getDatabaseName),
                    indexMetadata.getParentTableName,
                    indexMetadata.getIndexesMap.get(indexTableName).asScala.toList,
                    indexTableName)

                  try {
                    CarbonInternalScalaUtil
                      .LoadToSITable(sparkSession,
                        carbonLoadModel,
                        indexTableName,
                        isLoadToFailedSISegments = true,
                        secondaryIndex,
                        carbonTable)
                  } catch {
                    case ex: Exception =>
                      // in case of SI load only for for failed segments, catch the exception, but
                      // do not fail the main table load, as main table segments should be available
                      // for query
                      LOGGER.error(s"Load to SI table to $indexTableName is failed", ex)
                      return
                  }

                  // enable the SI table if it was disabled earlier due to failure during SI
                  // creation time
                  sparkSession.sql(
                    s"""ALTER TABLE ${carbonLoadModel.getDatabaseName}.$indexTableName SET
                       |SERDEPROPERTIES ('isSITableEnabled' = 'true')""".stripMargin)
                }
            }
          }
        }
    }
  }
}
