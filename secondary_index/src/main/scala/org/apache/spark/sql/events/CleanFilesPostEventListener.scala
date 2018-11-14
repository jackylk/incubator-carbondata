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
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{CleanFilesPostEvent, Event, OperationContext,
OperationEventListener}

/**
 *
 */
class CleanFilesPostEventListener extends OperationEventListener with Logging {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case cleanFilesPostEvent: CleanFilesPostEvent =>
        LOGGER.info("Clean files post event listener called")
        val carbonTable = cleanFilesPostEvent.carbonTable
        val indexTables = CarbonInternalScalaUtil
          .getIndexCarbonTables(carbonTable, cleanFilesPostEvent.sparkSession)
        FileInternalUtil
          .cleanIndexFiles(carbonTable, indexTables, carbonTable.getTablePath, true)
    }
  }
}
