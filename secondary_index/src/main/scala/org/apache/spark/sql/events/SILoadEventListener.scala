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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.command.{SecondaryIndex, SecondaryIndexModel}
import org.apache.spark.util.si.FileInternalUtil
import org.apache.spark.util.CarbonInternalCommonUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent
import org.apache.carbondata.spark.core.metadata.IndexMetadata
import org.apache.carbondata.spark.rdd.SecondaryIndexCreator

/**
 *
 */
class SILoadEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePreStatusUpdateEvent =>
        LOGGER.audit("Load pre status update event-listener called")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePreStatusUpdateEvent]
        val carbonLoadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        val sparkSession = SparkSession.getActiveSession.get
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach {
              indexTableName =>
                val secondaryIndex = SecondaryIndex(Some(carbonTable.getDatabaseName),
                  indexMetadata.getParentTableName,
                  indexMetadata.getIndexesMap.get(indexTableName).asScala.toList,
                  indexTableName)
                val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang
                .Long] = scala.collection.mutable
                  .Map((carbonLoadModel.getSegmentId, carbonLoadModel.getFactTimeStamp))
                val secondaryIndexModel = SecondaryIndexModel(
                  sparkSession.sqlContext,
                  carbonLoadModel,
                  carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
                  secondaryIndex,
                  List(carbonLoadModel.getSegmentId),
                  segmentIdToLoadStartTimeMapping)

                val factTablePath = CarbonProperties.getStorePath +
                                    CarbonCommonConstants.FILE_SEPARATOR +
                                    secondaryIndex.databaseName +
                                    CarbonCommonConstants.FILE_SEPARATOR +
                                    secondaryIndex.indexTableName
                val operationContext = new OperationContext
                val loadTableSIPreExecutionEvent: LoadTableSIPreExecutionEvent =
                  new LoadTableSIPreExecutionEvent(sparkSession,
                    new CarbonTableIdentifier(carbonTable.getDatabaseName, indexTableName, ""),
                    carbonLoadModel,
                    factTablePath)
                OperationListenerBus.getInstance
                  .fireEvent(loadTableSIPreExecutionEvent, operationContext)

                val segmentToSegmentFileNameMap: java.util.Map[String, String] = new java.util
                .HashMap[String, String]()
                val indexCarbonTable = SecondaryIndexCreator
                  .createSecondaryIndex(secondaryIndexModel, segmentToSegmentFileNameMap, true)

                val tableStatusUpdation = FileInternalUtil.updateTableStatus(
                  secondaryIndexModel.validSegments,
                  secondaryIndexModel.carbonLoadModel.getDatabaseName,
                  secondaryIndexModel.secondaryIndex.indexTableName,
                  SegmentStatus.SUCCESS,
                  secondaryIndexModel.segmentIdToLoadStartTimeMapping,
                  segmentToSegmentFileNameMap,
                  indexCarbonTable)

                // merge index files
                CarbonInternalCommonUtil.mergeIndexFiles(sparkSession.sparkContext,
                  secondaryIndexModel.validSegments,
                  segmentToSegmentFileNameMap,
                  indexCarbonTable.getTablePath,
                  indexCarbonTable, false)

                val loadTableACLPostExecutionEvent: LoadTableSIPostExecutionEvent =
                  new LoadTableSIPostExecutionEvent(sparkSession,
                    indexCarbonTable.getCarbonTableIdentifier,
                    carbonLoadModel)
                OperationListenerBus.getInstance
                  .fireEvent(loadTableACLPostExecutionEvent, operationContext)

                if (!tableStatusUpdation) {
                  throw new Exception("Table status updation failed while creating secondary index")
                }
            }
          } else {
            logInfo(s"No index tables found for table: ${
              carbonTable.getTableName
            }")
          }
        } else {
          logInfo(s"Index information is null for table: ${
            carbonTable.getTableName
          }")
        }
    }
  }
}
