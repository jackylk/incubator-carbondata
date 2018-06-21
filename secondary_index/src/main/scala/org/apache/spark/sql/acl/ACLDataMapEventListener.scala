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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.acl.ACLFileUtils.{setACLGroupRights, setPermissions}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.events.{CreateDataMapPostExecutionEvent,
  CreateDataMapPreExecutionEvent, _}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLDataMapEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val folderListBeforeOperation = "folderListBeforeOperation"
  val pathArrBeforeOperation = "pathArrBeforeOperation"

  /**
   * The class will handle the Create DataMap Events, to apply he file permission
   * on the _system folder and schema and datamap schema datamapstatus file.
   */
  class ACLPreDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = createDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = createDataMapPreExecutionEvent.storePah
          if (!FileFactory.isFileExist(systemDirectoryPath)) {
            CarbonUserGroupInformation.getInstance.getCurrentUser
              .doAs(new PrivilegedExceptionAction[Unit]() {
                override def run(): Unit = {
                  FileFactory.createDirectoryAndSetPermission(systemDirectoryPath,
                    ACLFileUtils.getPermissionsOnDatabase())
                }
              })
            val path = new Path(systemDirectoryPath)
            val hdfs = path.getFileSystem(sparkSession.sqlContext.sparkContext.hadoopConfiguration)
            setPermissions(hdfs, path)
            setACLGroupRights(CarbonUserGroupInformation.getInstance.getCurrentUser, hdfs, path)
          }
          val folderListBeforeReBuild = List[String](systemDirectoryPath)
          val pathArrBeforeLoadOperation = ACLFileUtils
            .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild)
          operationContext.setProperty(folderListBeforeOperation, folderListBeforeReBuild)
          operationContext.setProperty(pathArrBeforeOperation, pathArrBeforeLoadOperation)
        case updateDataMapStatusPreExecutionEvent: UpdateDataMapStatusPreExecutionEvent =>
          val sparkSession: SparkSession = updateDataMapStatusPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = updateDataMapStatusPreExecutionEvent.storePah
          val path = new Path(systemDirectoryPath)
          val hdfs = path.getFileSystem(sparkSession.sqlContext.sparkContext.hadoopConfiguration)
          setPermissions(hdfs, path)
          setACLGroupRights(CarbonUserGroupInformation.getInstance.getCurrentUser, hdfs, path)
      }
    }
  }

  class ACLPostDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPostExecutionEvent : CreateDataMapPostExecutionEvent =>
          val sparkSession = createDataMapPostExecutionEvent.sparkSession
          ACLFileUtils.takeSnapAfterOperationAndApplyACL(sparkSession, operationContext)
        case updateDataMapStatusPostExecutionEvent : UpdateDataMapStatusPostExecutionEvent =>
          val sparkSession = updateDataMapStatusPostExecutionEvent.sparkSession
          ACLFileUtils.takeSnapAfterOperationAndApplyACL(sparkSession, operationContext)
      }
    }
  }
}
