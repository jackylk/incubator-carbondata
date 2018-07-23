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
import org.apache.carbondata.events.{CreateCarbonRelationPostEvent, Event, OperationContext, OperationEventListener}


/**
 *
 */
class CreateCarbonRelationEventListener extends OperationEventListener with Logging {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case createCarbonRelationPostEvent: CreateCarbonRelationPostEvent =>
        LOGGER.debug("Create carbon relation post event listener called")
        val carbonTable = createCarbonRelationPostEvent.carbonTable
        val databaseName = createCarbonRelationPostEvent.carbonTable.getDatabaseName
        val tableName = createCarbonRelationPostEvent.carbonTable.getTableName
        val sparkSession = createCarbonRelationPostEvent.sparkSession
        CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    }
  }
}
