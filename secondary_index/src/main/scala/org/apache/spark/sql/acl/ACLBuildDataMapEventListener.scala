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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{BuildDataMapPreExecutionEvent, _}
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.spark.acl.{CarbonUserGroupInformation, InternalCarbonConstant}

/**
 * Listener's to handle the BuildDataMapEvents
 */
object ACLBuildDataMapEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreBuildDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {

      event match {
        case buildDataMapPreExecutionEvent : BuildDataMapPreExecutionEvent =>
          val tableIdentifier = buildDataMapPreExecutionEvent.identifier
          val sparkSession = buildDataMapPreExecutionEvent.sparkSession
          val dataMapName = buildDataMapPreExecutionEvent.dataMapNames
          listFoldersBeforeOperation(tableIdentifier, sparkSession, dataMapName)
      }
      /**
       * The method validate the existence of dataload group and takes the folder
       * snapshot before the operation.
       * @param tableIdentifier
       * @param sparkSession
       * @param dataMapNames
       */
      def listFoldersBeforeOperation(tableIdentifier: AbsoluteTableIdentifier,
          sparkSession: SparkSession,
          dataMapNames: mutable.Seq[String]): Unit = {
        if (!ACLFileUtils.isCarbonDataLoadGroupExist(sparkSession.sparkContext)) {
          val carbonDataLoadGroup = CarbonProperties.getInstance.
            getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
              InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT)
          val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser.getShortUserName
          throw PreEventException(s"CarbonDataLoad Group: $carbonDataLoadGroup " +
                                  s"is not set for the user $currentUser", false)
        }
        val folderListBeforeReBuild = dataMapNames.map(dataMapName => {
          tableIdentifier.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + dataMapName
        }).toList
        val pathArrBeforeLoadOperation = ACLFileUtils
          .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild,
            recursive = true)
        val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.getDatabaseName,
          tableIdentifier.getTableName, "")
        operationContext
          .setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier),
            folderListBeforeReBuild)
        operationContext
          .setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
            pathArrBeforeLoadOperation)
      }
    }
  }

  class ACLPostBuildDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val buildDataMapPostExecutionEvent = event.asInstanceOf[BuildDataMapPostExecutionEvent]
      val sparkSession = buildDataMapPostExecutionEvent.sparkSession
      // take the snapshot post refresh table event and apply acl
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          buildDataMapPostExecutionEvent.identifier.getCarbonTableIdentifier, true)
    }
  }

}
