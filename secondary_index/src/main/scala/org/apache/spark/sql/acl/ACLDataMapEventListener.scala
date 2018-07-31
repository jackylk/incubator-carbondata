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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.acl.ACLFileUtils.{getFolderListKey, getPathListKey}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.events.{CreateDataMapPostExecutionEvent, CreateDataMapPreExecutionEvent, _}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLDataMapEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * The class will handle the Create DataMap Events, to apply he file permission
   * on the _system folder and schema and datamap schema datamapstatus file.
   */
  class ACLPreDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = createDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = createDataMapPreExecutionEvent.storePath
          val tableIdentifier = createDataMapPreExecutionEvent.tableIdentifier
          val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.database
            .getOrElse(sparkSession.catalog.currentDatabase), tableIdentifier.table, "")
          if (!FileFactory.isFileExist(systemDirectoryPath)) {
            CarbonUserGroupInformation.getInstance.getCurrentUser
              .doAs(new PrivilegedExceptionAction[Unit]() {
                override def run(): Unit = {
                  FileFactory.createDirectoryAndSetPermission(systemDirectoryPath,
                    ACLFileUtils.getPermissionsOnDatabase())
                }
              })
          }
          val folderListBeforeReBuild = List[String](systemDirectoryPath)
          val pathArrBeforeLoadOperation = ACLFileUtils
            .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild)
          operationContext.setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier),
            folderListBeforeReBuild)
          operationContext.setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
            pathArrBeforeLoadOperation)
        case updateDataMapPreExecutionEvent: UpdateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = updateDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = updateDataMapPreExecutionEvent.storePath
          val tableIdentifier = updateDataMapPreExecutionEvent.tableIdentifier
          if (tableIdentifier != null) {
            val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase), tableIdentifier.table, "")
            val folderListBeforeReBuild = List[String](systemDirectoryPath)
            val pathArrBeforeLoadOperation = ACLFileUtils
              .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild)
            operationContext
              .setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier),
                folderListBeforeReBuild)
            operationContext
              .setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
                pathArrBeforeLoadOperation)
          }
      }
    }
  }

  class ACLPostDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPostExecutionEvent: CreateDataMapPostExecutionEvent =>
          val sparkSession = createDataMapPostExecutionEvent.sparkSession
          val tableIdentifier = createDataMapPostExecutionEvent.tableIdentifier
          if (tableIdentifier.isDefined) {
            val absoluteTableIdentifier = CarbonEnv
              .getCarbonTable(tableIdentifier.get.database, tableIdentifier.get.table)(sparkSession)
            val carbonTableIdentifier = absoluteTableIdentifier.getCarbonTableIdentifier
            val dmProviderName = createDataMapPostExecutionEvent.dmProviderName
            // Checking only for preaggregate as for preagregate only requires to take snapshot
            // of schema file. PredataMapEvent is already skipped in case of Preaggreagte and here
            // setting only schema file to change ownership and Permission
            if (dmProviderName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.toString)) {
              val schemaPath = absoluteTableIdentifier.getTablePath +
                               CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
                               CarbonCommonConstants.FILE_SEPARATOR + "schema"
              operationContext
                .setProperty(getFolderListKey(absoluteTableIdentifier.getCarbonTableIdentifier),
                  List(schemaPath))
              operationContext
                .setProperty(getPathListKey(absoluteTableIdentifier.getCarbonTableIdentifier),
                  ArrayBuffer(""))
            }
            ACLFileUtils
              .takeSnapAfterOperationAndApplyACL(sparkSession,
                operationContext,
                carbonTableIdentifier)
          }

        case updateDataMapPostExecutionEvent: UpdateDataMapPostExecutionEvent =>
          val sparkSession = updateDataMapPostExecutionEvent.sparkSession
          val tableIdentifier = updateDataMapPostExecutionEvent.tableIdentifier
          if (tableIdentifier != null) {
            val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase), tableIdentifier.table, "")
            ACLFileUtils
              .takeSnapAfterOperationAndApplyACL(sparkSession,
                operationContext,
                carbonTableIdentifier)
          }
      }
    }
  }

}
