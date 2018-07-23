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
import org.apache.carbondata.events.{AlterTableAddColumnPreEvent, _}

object ACLAlterTableDropColumnEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreAlterTableDropColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTableAddColumnPreEvent = event.asInstanceOf[AlterTableDropColumnPreEvent]
      val carbonTable: CarbonTable = alterTableAddColumnPreEvent.carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
      val sparkSession: SparkSession = alterTableAddColumnPreEvent.sparkSession
      val carbonTablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath

      ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostAlterTableDropColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTablePostExecutionEvent = event.asInstanceOf[AlterTableDropColumnPostEvent]
      val sparkSession = alterTablePostExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          alterTablePostExecutionEvent.carbonTable.getCarbonTableIdentifier)
    }
  }

  class ACLAbortAlterTableDropColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTableAborttExecutionEvent = event.asInstanceOf[AlterTableDropColumnAbortEvent]
    }
  }
}
