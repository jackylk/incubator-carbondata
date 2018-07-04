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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.events.{CreateDatabasePostExecutionEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLCreateDatabaseListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreCreateDatabaseListener extends OperationEventListener {

    override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {

    }
  }

  class ACLPostCreateDatabaseListener extends OperationEventListener {

    override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
      val createDatabasePostExecutionEvent = event.asInstanceOf[CreateDatabasePostExecutionEvent]
      val dataBasePath: String = createDatabasePostExecutionEvent.dataBasePath
//      val loginUser: UserGroupInformation = CarbonUserGroupInformation.getInstance.getLoginUser
      val currentUser: UserGroupInformation = CarbonUserGroupInformation.getInstance.getCurrentUser
      val currentUserName = currentUser.getShortUserName
      currentUser
        .doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            org.apache.carbondata.spark.acl.ACLFileUtils
              .setPermission(dataBasePath,
                ACLFileUtils.getPermissionsOnDatabase())
          }
        })
    }
  }

  class ACLAbortCreateDatabaseListener extends OperationEventListener {

    override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    }
  }

}
