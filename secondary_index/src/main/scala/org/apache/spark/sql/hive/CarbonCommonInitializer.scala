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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.acl.{ACLIUDDeleteEventListener, ACLIUDUpdateEventListener, ACLRefreshTableEventListener, CarbonEnvInitPreEventListener, _}
import org.apache.spark.sql.events.{DeleteFromTableEventListener, _}
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonPluginProperties
import org.apache.carbondata.events.{RefreshTablePostExecutionEvent, _}
import org.apache.carbondata.processing.loading.events.LoadEvents._
import org.apache.carbondata.spark.acl.ACLFileFactory

/**
 * Initializes the listener
 */
object CarbonCommonInitializer {
  var initialized = false

  def init(sparkSession: SparkSession): Unit = {
    if (!initialized) {
      CarbonEnv.init(sparkSession)
      // register internal carbon property to propertySet
      CarbonPluginProperties.validateAndLoadDefaultInternalProperties()

      val operationListenerBus = OperationListenerBus.getInstance()

      // Listeners added for blocking features(insert overwrite, bucketing, partition, complex type)
      if(SparkUtil.isFI) {
        operationListenerBus
          .addListener(classOf[CreateTablePreExecutionEvent],
            new BlockEventListener
          )
        operationListenerBus
          .addListener(classOf[LoadTablePreExecutionEvent],
            new BlockEventListener
          )
      }

      // ACL Listeners
      FileFactory.setFileTypeInerface(new ACLFileFactory())
      operationListenerBus
        .addListener(classOf[LoadTablePreExecutionEvent],
          new ACLLoadEventListener.ACLPreLoadEventListener
        )
      operationListenerBus
        .addListener(classOf[LoadTablePostExecutionEvent],
          new ACLLoadEventListener.ACLPostLoadEventListener
        )
      operationListenerBus
        .addListener(classOf[LoadTableAbortExecutionEvent],
          new ACLLoadEventListener.ACLAbortLoadEventListener
        )

      operationListenerBus
        .addListener(classOf[CreateTablePreExecutionEvent],
          new ACLCreateTableEventListener.ACLPreCreateTableEventListener
        )
      operationListenerBus
        .addListener(classOf[CreateTablePostExecutionEvent],
          new ACLCreateTableEventListener.ACLPostCreateTableEventListener
        )
      operationListenerBus
        .addListener(classOf[CreateTableAbortExecutionEvent],
          new ACLCreateTableEventListener.ACLAbortCreateTableEventListener
        )

      operationListenerBus
        .addListener(classOf[AlterTableAddColumnPreEvent],
          new ACLAlterTableAddColumnEventListener.ACLPreAlterTableAddColumnEventListener
        )
      operationListenerBus
        .addListener(classOf[AlterTableAddColumnPostEvent],
          new ACLAlterTableAddColumnEventListener.ACLPostAlterTableAddColumnEventListener
        )

      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPreEvent],
          new ACLAlterTableDropColumnEventListener.ACLPreAlterTableDropColumnEventListener
        )
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPostEvent],
          new ACLAlterTableDropColumnEventListener.ACLPostAlterTableDropColumnEventListener
        )
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnAbortEvent],
          new ACLAlterTableDropColumnEventListener.ACLAbortAlterTableDropColumnEventListener
        )

      operationListenerBus
        .addListener(classOf[AlterTableDataTypeChangePreEvent],
          new ACLAlterTableDataTypeChangeEventListener
          .ACLPreAlterTableDataTypeChangeEventListener
        )

      operationListenerBus
        .addListener(classOf[AlterTableDataTypeChangePostEvent],
          new ACLAlterTableDataTypeChangeEventListener
          .ACLPostAlterTableDataTypeChangeEventListener
        )

      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePreEvent],
          new ACLDeleteSegmentByDateEventListener.ACLPreDeleteSegmentByDateEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePostEvent],
          new ACLDeleteSegmentByDateEventListener.ACLPostDeleteSegmentByDateEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDateAbortEvent],
          new ACLDeleteSegmentByDateEventListener.ACLAbortDeleteSegmentByDateEventListener
        )

      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPreEvent],
          new ACLDeleteSegmentByIdEventListener.ACLPreDeleteSegmentByIdEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPostEvent],
          new ACLDeleteSegmentByIdEventListener.ACLPostDeleteSegmentByIdEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdAbortEvent],
          new ACLDeleteSegmentByIdEventListener.ACLAbortDeleteSegmentByIdEventListener
        )

      operationListenerBus
        .addListener(classOf[CleanFilesPreEvent],
          new ACLCleanFilesEventListener.ACLPreCleanFilesEventListener
        )
      operationListenerBus
        .addListener(classOf[CleanFilesPostEvent],
          new ACLCleanFilesEventListener.ACLPostCleanFilesEventListener
        )
      operationListenerBus
        .addListener(classOf[CleanFilesAbortEvent],
          new ACLCleanFilesEventListener.ACLAbortCleanFilesEventListener
        )

      operationListenerBus
        .addListener(classOf[AlterTableCompactionPreEvent],
          new ACLCompactionEventListener.ACLPreCompactionEventListener
        )
      operationListenerBus
        .addListener(classOf[AlterTableCompactionPostEvent],
          new ACLCompactionEventListener.ACLPostCompactionEventListener
        )
      operationListenerBus
        .addListener(classOf[AlterTableCompactionAbortEvent],
          new ACLCompactionEventListener.ACLAbortCompactionEventListener
        )

      operationListenerBus
        .addListener(classOf[CreateDatabasePreExecutionEvent],
          new ACLCreateDatabaseListener.ACLPreCreateDatabaseListener
        )
      operationListenerBus
        .addListener(classOf[CreateDatabasePostExecutionEvent],
          new ACLCreateDatabaseListener.ACLPostCreateDatabaseListener
        )
      operationListenerBus
        .addListener(classOf[CreateDatabaseAbortExecutionEvent],
          new ACLCreateDatabaseListener.ACLAbortCreateDatabaseListener
        )

      operationListenerBus
        .addListener(classOf[DeleteFromTablePreEvent],
          new ACLIUDDeleteEventListener.ACLPreIUDDeleteEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteFromTablePostEvent],
          new ACLIUDDeleteEventListener.ACLPostIUDDeleteEventListener
        )
      operationListenerBus
        .addListener(classOf[DeleteFromTableAbortEvent],
          new ACLIUDDeleteEventListener.ACLAbortIUDDeleteEventListener
        )

      operationListenerBus
        .addListener(classOf[UpdateTablePreEvent],
          new ACLIUDUpdateEventListener.ACLPreIUDUpdateEventListener
        )
      operationListenerBus
        .addListener(classOf[UpdateTablePostEvent],
          new ACLIUDUpdateEventListener.ACLPostIUDUpdateEventListener
        )
      operationListenerBus
        .addListener(classOf[UpdateTableAbortEvent],
          new ACLIUDUpdateEventListener.ACLAbortIUDUpdateEventListener
        )

      operationListenerBus
        .addListener(classOf[CarbonEnvInitPreEvent],
          new CarbonEnvInitPreEventListener
        )

      // SI Listeners
      operationListenerBus
        .addListener(classOf[LoadTablePreStatusUpdateEvent], new SILoadEventListener)
      operationListenerBus
        .addListener(classOf[LookupRelationPostEvent], new SIRefreshEventListener)
      // TODO: get create relation event
      operationListenerBus
        .addListener(classOf[CreateCarbonRelationPostEvent], new
            CreateCarbonRelationEventListener
        )
      operationListenerBus
        .addListener(classOf[DropTablePreEvent], new SIDropEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPreEvent], new AlterTableDropColumnEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableRenamePostEvent], new AlterTableRenameEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPostEvent], new DeleteSegmentByIdListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePostEvent], new DeleteSegmentByDateListener)
      operationListenerBus
        .addListener(classOf[CleanFilesPostEvent], new CleanFilesPostEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
          new AlterTableCompactionPostEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableCompactionExceptionEvent],
          new AlterTableCompactionExceptionSIEventListener)
      operationListenerBus
        .addListener(classOf[UpdateTablePreEvent], new UpdateTablePreEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTablePostEvent], new DeleteFromTableEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTablePreEvent], new DeleteFromTableEventListener)
      // refresh table listner
      operationListenerBus
        .addListener(classOf[RefreshTablePreExecutionEvent],
          new ACLRefreshTableEventListener.ACLPreRefreshTableEventListener
        )
      operationListenerBus
        .addListener(classOf[RefreshTablePostExecutionEvent],
          new ACLRefreshTableEventListener.ACLPostRefreshTableEventListener
        )
      initialized = true
    }
  }
}
