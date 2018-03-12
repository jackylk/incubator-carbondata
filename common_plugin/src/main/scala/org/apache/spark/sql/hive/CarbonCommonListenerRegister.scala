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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.events._

import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent

/**
 * This class registers common listeners
 */
class CarbonCommonListenerRegister {

  // initialize all common listeners
  CarbonCommonListenerRegister.init

}

// Register all the required listeners using the singleton instance as the listeners
// need to be registered only once
object CarbonCommonListenerRegister {
  var initialized = false

  def init: Unit = {
    if (!initialized) {
      val operationListenerBus = OperationListenerBus.getInstance()
      // Merge index listeners
      operationListenerBus
        .addListener(classOf[LoadTablePreStatusUpdateEvent], new MergeIndexEventListener)
      // Merge compaction listeners
      operationListenerBus
        .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
          new MergeIndexCompactionEventListener)
      // Merge index compaction DDL listener
      operationListenerBus
        .addListener(classOf[AlterTableCompactionExceptionEvent],
          new AlterTableCompactionExceptionEventListener)
      initialized = true
    }
  }
}
