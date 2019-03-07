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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.cache.CarbonDropCacheCommand
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DropCacheEvent, Event, OperationContext, OperationEventListener}


object DropCacheSIEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {

    event match {
      case dropCacheEvent: DropCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val sparkSession = dropCacheEvent.sparkSession
        val internalCall = dropCacheEvent.internalCall
        if (CarbonInternalScalaUtil.isIndexTable(carbonTable) && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        val allIndexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
        val dbName = carbonTable.getDatabaseName
        for (indexTableName <- allIndexTables.asScala) {
          try {
            val dropCacheCommandForChildTable =
              CarbonDropCacheCommand(
                TableIdentifier(indexTableName, Some(dbName)),
                internalCall = true)
            dropCacheCommandForChildTable.processMetadata(sparkSession)
          }
          catch {
            case e: Exception =>
              LOGGER.error(s"Clean cache for SI table $indexTableName failed. ", e)
          }
        }

    }
  }
}
