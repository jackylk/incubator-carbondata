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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.events._
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

/**
 * The Listener implementation for the RefreshTable Pre and Post Operation Event
 */
object ACLRefreshTableEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * This is to apply table permission on table folder and take snapshot
   * of table folder before the Refresh event
   */
  class ACLPreRefreshTableEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      val refreshTablePreExecutionEvent = event.asInstanceOf[RefreshTablePreExecutionEvent]
      val absoluteTableIdentifier: AbsoluteTableIdentifier = refreshTablePreExecutionEvent
        .identifier
      val sparkSession: SparkSession = refreshTablePreExecutionEvent.sparkSession
      takeSnapshotBeforeOperation(operationContext, sparkSession, absoluteTableIdentifier)
    }

    /**
     *  The method adds acl to the table folder
     *  Take snapshot of only table folder to avoid adding data load group acl to table folder
     *
     * @param operationContext
     * @param sparkSession
     * @param absoluteTableIdentifier
     */
    def takeSnapshotBeforeOperation(operationContext: OperationContext,
        sparkSession: SparkSession,
        absoluteTableIdentifier: AbsoluteTableIdentifier): Unit = {
      val carbonTablePath = absoluteTableIdentifier.getTablePath
      val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
      currentUser.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          // Set permission on the table permission on table folder
          org.apache.carbondata.spark.acl.ACLFileUtils.setPermission(carbonTablePath,
            ACLFileUtils.getPermissionsOnTable())
        }
      })
      // get path of all possible depths
      val folderListBeforeCreate: List[String] = ACLFileUtils
        .getTablePathListForSnapshot(carbonTablePath, null)
      // get the snapshot of the table folder
      val carbonTableIdentifier = new CarbonTableIdentifier(absoluteTableIdentifier.getDatabaseName,
        absoluteTableIdentifier.getTableName, "")
      val pathArrBeforeCreateOperation = ACLFileUtils
        .takeNonRecursiveSnapshot(sparkSession.sqlContext, new Path(carbonTablePath))
      operationContext
        .setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier), folderListBeforeCreate)
      operationContext
        .setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
          pathArrBeforeCreateOperation)
    }
  }

  /**
   * The class is take snapshot and apply acl post refresh table event
   */
  class ACLPostRefreshTableEventListener extends OperationEventListener {
    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      val refreshTablePostExecutionEvent = event.asInstanceOf[RefreshTablePostExecutionEvent]
      val sparkSession = refreshTablePostExecutionEvent.sparkSession
      // take the snapshot post refresh table event and apply acl
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          refreshTablePostExecutionEvent.identifier.getCarbonTableIdentifier)
    }
  }

}
