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
        val internalCall = showTableCacheEvent.internalCall
        if (CarbonInternalScalaUtil.isIndexTable(carbonTable) && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on index table.")
        }

        val childTables = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[List[(String, String)]]

        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          operationContext.setProperty(carbonTable.getTableUniqueName, indexTables.map {
            indexTable => (carbonTable.getDatabaseName + "-" +indexTable, "Secondary Index")
          }.toList ++ childTables)
        }
    }
  }
}
