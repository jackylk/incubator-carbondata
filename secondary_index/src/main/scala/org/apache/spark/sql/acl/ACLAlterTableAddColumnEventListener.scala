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
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableAddColumnPreEvent, _}

object ACLAlterTableAddColumnEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreAlterTableAddColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTableAddColumnPreEvent = event.asInstanceOf[AlterTableAddColumnPreEvent]
      val carbonTable: CarbonTable = alterTableAddColumnPreEvent.carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
      val sparkSession: SparkSession = alterTableAddColumnPreEvent.sparkSession
      val carbonTablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath


     ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostAlterTableAddColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTablePostExecutionEvent = event.asInstanceOf[AlterTableAddColumnPostEvent]
      val sparkSession = alterTablePostExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          alterTablePostExecutionEvent.carbonTable.getCarbonTableIdentifier)

    }
  }
}
