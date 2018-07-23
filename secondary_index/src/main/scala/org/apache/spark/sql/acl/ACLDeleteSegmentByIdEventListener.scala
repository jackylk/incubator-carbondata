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
import org.apache.carbondata.events.{DeleteSegmentByIdAbortEvent, DeleteSegmentByIdPostEvent, DeleteSegmentByIdPreEvent, _}

object ACLDeleteSegmentByIdEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreDeleteSegmentByIdEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPreEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
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

  class ACLPostDeleteSegmentByIdEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPostEvent = event.asInstanceOf[DeleteSegmentByIdPostEvent]
      val sparkSession = deleteSegmentByIdPostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          deleteSegmentByIdPostEvent.carbonTable.getCarbonTableIdentifier)
    }

    }
  class ACLAbortDeleteSegmentByIdEventListener extends OperationEventListener {
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdAbortEvent = event.asInstanceOf[DeleteSegmentByIdAbortEvent]
    }
  }
}

