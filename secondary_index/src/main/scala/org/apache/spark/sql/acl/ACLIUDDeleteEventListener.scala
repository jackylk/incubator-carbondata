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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{DeleteFromTableAbortEvent, DeleteFromTablePostEvent, DeleteFromTablePreEvent, _}

object ACLIUDDeleteEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreIUDDeleteEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteFromTablePreEvent = event.asInstanceOf[DeleteFromTablePreEvent]
      val carbonTable: CarbonTable = deleteFromTablePreEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = deleteFromTablePreEvent.sparkSession
      val carbonTablePath = carbonTable.getTablePath
      ACLFileUtils.takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
          carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostIUDDeleteEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteFromTablePostEvent = event.asInstanceOf[DeleteFromTablePostEvent]
      val sparkSession = deleteFromTablePostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          deleteFromTablePostEvent.carbonTable.getCarbonTableIdentifier)
    }
  }

  class ACLAbortIUDDeleteEventListener extends OperationEventListener {
    /**
     * Called on a specified event occurrence
     *
     * @param event
     * @param operationContext
     */
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteFromTableAbortEvent = event.asInstanceOf[DeleteFromTableAbortEvent]
    }
  }
}
