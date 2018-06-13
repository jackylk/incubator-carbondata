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

package org.apache.spark.sql.events

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.acl.ACLFileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.events._

object ACLIndexLoadEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val folderListBeforeOperation = "folderListBeforeOperation"
  val pathArrBeforeOperation = "pathArrBeforeOperation"

  class ACLPreCreateIndexEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val createSITablePreExecutionEvent = event.asInstanceOf[LoadTableSIPreExecutionEvent]
      val tablePath: String = createSITablePreExecutionEvent.tablePath
      val carbonTableIdentifier: CarbonTableIdentifier = createSITablePreExecutionEvent
        .carbonTableIdentifier
      val sparkSession: SparkSession = createSITablePreExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapshotBeforeOpeartion(operationContext, sparkSession, tablePath, null)
    }
  }

  class ACLPostCreateIndexEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTablePostExecutionEvent = event.asInstanceOf[LoadTableSIPostExecutionEvent]
      val sparkSession = alterTablePostExecutionEvent.sparkSession
      ACLFileUtils.takeSnapAfterOperationAndApplyACL(sparkSession, operationContext)

    }
  }

}
