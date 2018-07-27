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

import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{DeleteSegmentByIdAbortEvent, DeleteSegmentByIdPostEvent, DeleteSegmentByIdPreEvent, _}

object ACLDeleteSegmentByIdEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreDeleteSegmentByIdEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPreEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
      val carbonTable: CarbonTable = deleteSegmentByIdPreEvent
        .carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable
        .getCarbonTableIdentifier
      val sparkSession: SparkSession = deleteSegmentByIdPreEvent.sparkSession
      val carbonTablePath = carbonTable.getTablePath
      ACLFileUtils.takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
        carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostDeleteSegmentByIdEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdPostEvent = event.asInstanceOf[DeleteSegmentByIdPostEvent]
      val sparkSession = deleteSegmentByIdPostEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          deleteSegmentByIdPostEvent.carbonTable.getCarbonTableIdentifier)
    }

    }
  class ACLAbortDeleteSegmentByIdEventListener extends OperationEventListener {
    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val deleteSegmentByIdAbortEvent = event.asInstanceOf[DeleteSegmentByIdAbortEvent]
    }
  }
}

