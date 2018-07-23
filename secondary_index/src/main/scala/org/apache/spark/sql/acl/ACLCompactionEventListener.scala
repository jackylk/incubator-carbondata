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
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events._
import org.apache.carbondata.spark.acl.{CarbonUserGroupInformation, InternalCarbonConstant}

object ACLCompactionEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreCompactionEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val compactionPreExecutionEvent = event.asInstanceOf[AlterTableCompactionPreEvent]
      val carbonTable: CarbonTable = compactionPreExecutionEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = compactionPreExecutionEvent.sparkSession
      val carbonTablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath

      if (!ACLFileUtils.isCarbonDataLoadGroupExist(sparkSession.sparkContext)) {
        val carbonDataLoadGroup = CarbonProperties.getInstance.
          getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
            InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT)
        val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser.getShortUserName
        sys
          .error(s"CarbonDataLoad Group: $carbonDataLoadGroup is not set for the user $currentUser")
      }

      ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostCompactionEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val compactionPostExecutionEvent = event.asInstanceOf[AlterTableCompactionPostEvent]
      val sparkSession = compactionPostExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          compactionPostExecutionEvent.carbonTable.getCarbonTableIdentifier)

    }
  }

  class ACLAbortCompactionEventListener extends OperationEventListener {
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val compactionAbortExecutionEvent = event.asInstanceOf[AlterTableCompactionAbortEvent]
    }
  }

}
