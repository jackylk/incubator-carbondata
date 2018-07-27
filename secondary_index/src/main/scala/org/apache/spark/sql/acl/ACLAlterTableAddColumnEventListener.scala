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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableAddColumnPreEvent, _}

object ACLAlterTableAddColumnEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class ACLPreAlterTableAddColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTableAddColumnPreEvent = event.asInstanceOf[AlterTableAddColumnPreEvent]
      val carbonTable: CarbonTable = alterTableAddColumnPreEvent.carbonTable
      val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
      val sparkSession: SparkSession = alterTableAddColumnPreEvent.sparkSession
      val carbonTablePath = carbonTable.getAbsoluteTableIdentifier.getTablePath


     ACLFileUtils
          .takeSnapshotBeforeOperation(operationContext, sparkSession, carbonTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName), carbonTableIdentifier)
    }
  }

  class ACLPostAlterTableAddColumnEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val alterTablePostExecutionEvent = event.asInstanceOf[AlterTableAddColumnPostEvent]
      val sparkSession = alterTablePostExecutionEvent.sparkSession
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(sparkSession,
          operationContext,
          alterTablePostExecutionEvent.carbonTable.getCarbonTableIdentifier)

    }
  }
}
