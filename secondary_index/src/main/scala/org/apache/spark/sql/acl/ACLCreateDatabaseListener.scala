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
