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
import scala.collection.mutable

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.cache.CarbonShowCacheCommand
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, ShowTableCacheEvent}
import org.apache.carbondata.spark.core.metadata.IndexMetadata

object ShowCacheSIEventListener extends OperationEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>

        val carbonTable = showTableCacheEvent.carbonTable
        val sparkSession = showTableCacheEvent.sparkSession
        val internalCall = showTableCacheEvent.internalCall
        if (CarbonInternalScalaUtil.isIndexTable(carbonTable) && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on index table.")
        }

        val currentTableSizeMap = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[mutable.Map[String, (String, Long, Long)]]

        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach(indexTableName => {
              val childCarbonTable = CarbonEnv.getCarbonTable(
                TableIdentifier(indexTableName, Some(carbonTable.getDatabaseName)))(sparkSession)
              val resultForChild = CarbonShowCacheCommand(None, internalCall = true)
                .getTableCache(sparkSession, childCarbonTable)
              val datamapSize = resultForChild.head.getLong(1)
              currentTableSizeMap.put(indexTableName, ("secondary index", datamapSize, 0L))
            })
          }
        }
    }
  }
}
