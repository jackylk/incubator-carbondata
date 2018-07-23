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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, PostAlterTableHivePartitionCommandEvent, PreAlterTableHivePartitionCommandEvent}

/**
 *
 */
object AlterTableHivePartitionCommandEventListeners {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreAlterTableHivePartitionCommandEventListener extends OperationEventListener {
    /**
     * Called on a specified event occurrence
     *
     * @param event
     * @param operationContext
     */
    override protected def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val preAlterTableHivePartitionCommandEvent = event
        .asInstanceOf[PreAlterTableHivePartitionCommandEvent]
      val carbonTable: CarbonTable = preAlterTableHivePartitionCommandEvent.carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
      val sparkSession: SparkSession = preAlterTableHivePartitionCommandEvent.sparkSession
      val carbonTablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath
      val segmentFilesLocation = CarbonTablePath
        .getSegmentFilesLocation(carbonTablePath)
      if (carbonTable.isHivePartitionTable) {
        ACLLoadEventListener.createDirectoryAndSetGroupAcl(segmentFilesLocation)(sparkSession)
      }

      ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext,
            sparkSession,
            carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostAlterTableHivePartitionCommandEventListener extends OperationEventListener {
    /**
     * Called on a specified event occurrence
     *
     * @param event
     * @param operationContext
     */
    override protected def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val postAlterTableHivePartitionCommandEvent = event
        .asInstanceOf[PostAlterTableHivePartitionCommandEvent]
      val sparkSession = postAlterTableHivePartitionCommandEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          postAlterTableHivePartitionCommandEvent.carbonTable.getCarbonTableIdentifier)
    }
  }

}
