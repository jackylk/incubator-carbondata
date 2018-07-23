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


object ACLIUDUpdateEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreIUDUpdateEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val updateTablePreEvent = event.asInstanceOf[UpdateTablePreEvent]
      val carbonTable: CarbonTable = updateTablePreEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = updateTablePreEvent.sparkSession
      val carbonTablePath = carbonTable.getTablePath
      ACLFileUtils.takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
          carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostIUDUpdateEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val updateTablePostEvent = event.asInstanceOf[UpdateTablePostEvent]
      val sparkSession = updateTablePostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          updateTablePostEvent.carbonTable.getCarbonTableIdentifier)
    }
  }

  class ACLAbortIUDUpdateEventListener extends OperationEventListener {
    /**
     * Called on a specified event occurrence
     *
     * @param event
     * @param operationContext
     */
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val updateTableAbortEvent = event.asInstanceOf[UpdateTableAbortEvent]
    }
  }
}
