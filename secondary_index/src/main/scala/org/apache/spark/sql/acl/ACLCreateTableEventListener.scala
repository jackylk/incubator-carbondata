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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, Event, OperationEventListener}
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLCreateTableEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val folderListBeforeOperation = "folderListBeforeOperation"
  val pathArrBeforeOperation = "pathArrBeforeOperation"

  class ACLPreCreateTableEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      val createTablePreExecutionEvent = event.asInstanceOf[CreateTablePreExecutionEvent]
      val carbonTableIdentifier: CarbonTableIdentifier = createTablePreExecutionEvent.identifier
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = createTablePreExecutionEvent.sparkSession
      val tablePath: String = createTablePreExecutionEvent.identifier.getTablePath
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(tablePath, carbonTableIdentifier)
      CarbonUserGroupInformation.getInstance.getCurrentUser
        .doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            FileFactory.createDirectoryAndSetPermission(tablePath,
              ACLFileUtils.getPermissionsOnTable())
          }
        })
//      val folderListbeforeCreate: List[Path] = ACLFileUtils
//        .getTablePathListForSnapshot(carbonTablePath)
//      val pathArrBeforeCreateOperation = ACLFileUtils
//        .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListbeforeCreate)
//      operationContext.setProperty(folderListBeforeOperation, folderListbeforeCreate)
//      operationContext.setProperty(pathArrBeforeOperation, pathArrBeforeCreateOperation)
      ACLFileUtils.takeSnapshotBeforeOpeartion(operationContext, sparkSession, carbonTablePath)
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
      ACLFileUtils.takeSnapAfterOperationAndApplyACL(sparkSession, operationContext)

    }
  }

  class ACLAbortCreateTableEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {

    }
  }

}
