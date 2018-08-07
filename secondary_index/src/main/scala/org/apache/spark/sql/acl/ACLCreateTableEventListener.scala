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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.CarbonLockFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, Event, OperationEventListener}
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLCreateTableEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreCreateTableEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      val createTablePreExecutionEvent = event.asInstanceOf[CreateTablePreExecutionEvent]
      val carbonTableIdentifier: CarbonTableIdentifier = createTablePreExecutionEvent.identifier
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = createTablePreExecutionEvent.sparkSession
      val tablePath: String = createTablePreExecutionEvent.identifier.getTablePath
      CarbonUserGroupInformation.getInstance.getCurrentUser
        .doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            if (ACLFileUtils.isACLSupported(tablePath)) {
              FileFactory.createDirectoryAndSetPermission(tablePath,
                ACLFileUtils.getPermissionsOnTable())
            }
            // create the lock directory path during create table and before taking first snapshot
            // to avoid permission issue in acl for lock files
            val lockDirPath = if (
              CarbonProperties.getInstance().getProperty(CarbonCommonConstants.LOCK_PATH) !=
              null) {
              CarbonTablePath
                .getLockFilesDirPath(CarbonLockFactory
                  .getLockpath(carbonTableIdentifier.getTableId))
            } else {
              CarbonTablePath
                .getLockFilesDirPath(tablePath)
            }
            if (null != lockDirPath && !FileFactory.isFileExist(lockDirPath) &&
                ACLFileUtils.isACLSupported(lockDirPath)) {
              FileFactory
                .createDirectoryAndSetPermission(lockDirPath,
                  new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
            }
          }
        })
      ACLFileUtils
        .takeSnapshotBeforeOperation(operationContext,
          sparkSession,
          tablePath,
          null,
          carbonTableIdentifier)
    }
  }

  class ACLPostCreateTableEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      val createTablePostExecutionEvent = event.asInstanceOf[CreateTablePostExecutionEvent]
      val sparkSession = createTablePostExecutionEvent.sparkSession

//      val folderPathsBeforeCreate = operationContext.getProperty(folderListBeforeOperation)
//        .asInstanceOf[List[Path]]
//      val pathArrBeforeCreate = operationContext.getProperty(pathArrBeforeOperation)
//        .asInstanceOf[ArrayBuffer[String]]
//      val pathArrAfterCreate = ACLFileUtils
//        .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderPathsBeforeCreate)
//
//      ACLFileUtils.changeOwnerRecursivelyAfterOperation(sparkSession.sqlContext,
//        pathArrBeforeCreate, pathArrAfterCreate)
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          createTablePostExecutionEvent.identifier.getCarbonTableIdentifier)

    }
  }

  class ACLAbortCreateTableEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {

    }
  }

}
