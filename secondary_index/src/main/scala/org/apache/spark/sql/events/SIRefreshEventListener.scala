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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.CarbonInternalMetastore

import org.apache.carbondata.events.{Event, LookupRelationPostEvent, OperationContext,
OperationEventListener}

/**
 *
 */
class SIRefreshEventListener extends OperationEventListener with Logging {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case lookupRelationPostEvent: LookupRelationPostEvent =>
        val carbonTable = lookupRelationPostEvent.carbonTable
        val databaseName = lookupRelationPostEvent.carbonTable.getDatabaseName
        val tableName = lookupRelationPostEvent.carbonTable.getTableName
        val sparkSession = lookupRelationPostEvent.sparkSession
        CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    }
  }
}
