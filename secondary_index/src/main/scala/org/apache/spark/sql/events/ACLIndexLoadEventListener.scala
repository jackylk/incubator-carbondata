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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.acl.ACLFileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.events._

object ACLIndexLoadEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreCreateIndexEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val createSITablePreExecutionEvent = event.asInstanceOf[LoadTableSIPreExecutionEvent]
      val tablePath: String = createSITablePreExecutionEvent.indexCarbonTable.getMetadataPath
      val carbonTableIdentifier: CarbonTableIdentifier = createSITablePreExecutionEvent
        .carbonTableIdentifier
      val sparkSession: SparkSession = createSITablePreExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapshotBeforeOperation(operationContext,
          sparkSession,
          tablePath,
          null,
          carbonTableIdentifier,
          true)
    }
  }

  class ACLPostCreateIndexEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTablePostExecutionEvent = event.asInstanceOf[LoadTableSIPostExecutionEvent]
      val sparkSession = alterTablePostExecutionEvent.sparkSession
      val carbonTable = alterTablePostExecutionEvent.carbonTable
      ACLFileUtils.takeSnapAfterOperationAndApplyACL(sparkSession,
        operationContext,
        carbonTable.getCarbonTableIdentifier,
        false,
        true,
        Some(carbonTable)
      )

    }
  }
}
