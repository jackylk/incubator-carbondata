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
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.{CarbonInternalCommonUtil, CarbonInternalScalaUtil, Compactor}

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.events.{AlterTableCompactionPreStatusUpdateEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType, InternalCompactionType}


/**
 *
 */
class AlterTableCompactionPostEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableCompactionPostEvent: AlterTableCompactionPreStatusUpdateEvent =>
        LOGGER.audit("post load event-listener called")
        val carbonLoadModel = alterTableCompactionPostEvent.carbonLoadModel
        val sQLContext = alterTableCompactionPostEvent.sparkSession.sqlContext
        val compactionType: CompactionType = alterTableCompactionPostEvent.carbonMergerMapping
          .campactionType
        if (compactionType.toString
          .equalsIgnoreCase(InternalCompactionType.SEGMENT_INDEX.toString)) {
          val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
          if (null != indexTablesList && indexTablesList.nonEmpty) {
            indexTablesList.foreach { indexTableAndColumns =>
              val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
                carbonLoadModel.getTableName,
                indexTableAndColumns._2.asScala.toList,
                indexTableAndColumns._1)
              val metastore = CarbonEnv.getInstance(sQLContext.sparkSession)
                .carbonMetastore
              val indexCarbonTable = metastore
                .lookupRelation(Some(carbonLoadModel.getDatabaseName),
                  secondaryIndex.indexTableName)(sQLContext
                  .sparkSession).asInstanceOf[CarbonRelation].carbonTable

              // Just launch job to merge index for all index tables
              CarbonInternalCommonUtil.mergeIndexFiles(
                sQLContext.sparkContext,
                CarbonDataMergerUtil.getValidSegmentList(
                  indexCarbonTable.getAbsoluteTableIdentifier).asScala,
                indexCarbonTable.getTablePath,
                indexCarbonTable,
                true)
            }
          }
        } else {
          val mergedLoadName = alterTableCompactionPostEvent.mergedLoadName
          val loadMetadataDetails = new LoadMetadataDetails
          loadMetadataDetails.setLoadName(mergedLoadName)
          val loadsToMerge: Array[String] = alterTableCompactionPostEvent.carbonMergerMapping
            .validSegments
          val loadName = mergedLoadName
            .substring(mergedLoadName.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                       CarbonCommonConstants.LOAD_FOLDER.length)
          val mergeLoadStartTime = CarbonUpdateUtil.readCurrentTime()

          val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang
          .Long] = scala.collection.mutable.Map((loadName, mergeLoadStartTime))
          Compactor.createSecondaryIndexAfterCompaction(sQLContext,
            carbonLoadModel,
            List(loadName),
            loadsToMerge,
            segmentIdToLoadStartTimeMapping, true)
        }
      case _ =>
    }
  }
}
