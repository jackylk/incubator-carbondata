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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.CarbonInternalMetastore

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.events.{Event, LookupRelationPostEvent, OperationContext, OperationEventListener}

/**
 *
 */
class SIRefreshEventListener extends OperationEventListener with Logging {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case lookupRelationPostEvent: LookupRelationPostEvent =>
        LOGGER.debug("SI Refresh post event listener called")
        val carbonTable = lookupRelationPostEvent.carbonTable
        val databaseName = lookupRelationPostEvent.carbonTable.getDatabaseName
        val tableName = lookupRelationPostEvent.carbonTable.getTableName
        val sparkSession = lookupRelationPostEvent.sparkSession
        CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    }
  }
}
