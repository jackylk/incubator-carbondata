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

import org.apache.spark.sql.execution.command.CreateFunctionCommand

import org.apache.carbondata.events.{CarbonEnvInitPreEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants

/**
 *
 */
class CarbonEnvInitPreEventListener extends OperationEventListener {
   /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
     val carbonEnvInitPreEvent = event.asInstanceOf[CarbonEnvInitPreEvent]
     val storePath = carbonEnvInitPreEvent.storePath
     val sparkSession = carbonEnvInitPreEvent.sparkSession
     val carbonSessionInfo = carbonEnvInitPreEvent.carbonSessionInfo
     CarbonUserGroupInformation.getInstance.enableDriverUser
    carbonSessionInfo.getNonSerializableExtraInfo.put(CarbonInternalCommonConstants.USER_NAME,
      sparkSession.sessionState.catalog.getClientUser)
     Utils.initCarbonFoldersPermission(storePath, sparkSession)
     // register position ID UDF
     sparkSession.udf.register("getPositionId", () => "")
    CreateFunctionCommand(None, "NI", "org.apache.spark.sql.hive.NonIndexUDFExpression",
      Seq(), true).run(sparkSession)
  }
}
