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
import org.apache.carbondata.events._


object ACLDeleteSegmentByDateEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreDeleteSegmentByDateEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPreEvent = event.asInstanceOf[DeleteSegmentByDatePreEvent]
      val carbonTable: CarbonTable = deleteSegmentByIdPreEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = deleteSegmentByIdPreEvent.sparkSession
      val carbonTablePath = carbonTable.getTablePath
      ACLFileUtils.takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
        carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostDeleteSegmentByDateEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPostEvent = event.asInstanceOf[DeleteSegmentByDatePostEvent]
      val sparkSession = deleteSegmentByIdPostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          deleteSegmentByIdPostEvent.carbonTable.getCarbonTableIdentifier)
    }

    }
  class ACLAbortDeleteSegmentByDateEventListener extends OperationEventListener {
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdAbortEvent = event.asInstanceOf[DeleteSegmentByDateAbortEvent]
    }
  }
}

