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
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.events.{CleanFilesPostEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class CleanFilesPostEventListener extends OperationEventListener with Logging {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case cleanFilesPostEvent: CleanFilesPostEvent =>
        LOGGER.audit("Clean files post event listener called")
        val carbonTable = cleanFilesPostEvent.carbonTable
        val sparkSession = cleanFilesPostEvent.sparkSession
        FileInternalUtil.cleanIndexFiles(carbonTable, carbonTable.getTablePath, true)
    }
  }
}
