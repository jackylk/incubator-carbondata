/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
          buildDataMapPostExecutionEvent.identifier.getCarbonTableIdentifier)
    }
  }
}
