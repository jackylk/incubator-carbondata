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
package org.apache.spark.sql.acl

import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{CleanFilesPreEvent, _}


object ACLCleanFilesEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreCleanFilesEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val cleanFilesPreEvent = event.asInstanceOf[CleanFilesPreEvent]
      val carbonTable: CarbonTable = cleanFilesPreEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = cleanFilesPreEvent.sparkSession
      val carbonTablePath = carbonTable.getTablePath
      ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostCleanFilesEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPostEvent = event.asInstanceOf[CleanFilesPostEvent]
      val sparkSession = deleteSegmentByIdPostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          deleteSegmentByIdPostEvent.carbonTable.getCarbonTableIdentifier)
    }
    }
}

