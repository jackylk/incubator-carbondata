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

package org.apache.spark.util

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.command.{SecondaryIndex, SecondaryIndexModel}

import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.rdd.SecondaryIndexCreator
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil

/**
 *
 */
object Compactor {

  /**
   * This method will create secondary index for all the index tables after compaction is completed
   *
   * @param sqlContext
   * @param carbonLoadModel
   * @param validSegments
   * @param segmentIdToLoadStartTimeMapping
   */
  def createSecondaryIndexAfterCompaction(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      validSegments: scala.List[String],
      loadsToMerge: util.List[LoadMetadataDetails],
      segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
      forceAccessSegment: Boolean = false):
  Unit = {
    val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // get list from carbonTable.getIndexes method
    if (null == CarbonInternalScalaUtil.getIndexesMap(carbonMainTable)) {
      throw new Exception("Secondary index load failed")
    }
    val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
    indexTablesList.foreach { indexTableAndColumns =>
      val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
        carbonLoadModel.getTableName,
        indexTableAndColumns._2.asScala.toList,
        indexTableAndColumns._1)
      val secondaryIndexModel = SecondaryIndexModel(sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        validSegments,
        segmentIdToLoadStartTimeMapping)
      try {
        val carbonTable = SecondaryIndexCreator
          .createSecondaryIndex(secondaryIndexModel, forceAccessSegment)
        CarbonInternalLoaderUtil.updateLoadMetadataWithMergeStatus(loadsToMerge,
          carbonTable.getMetaDataFilepath,
          validSegments.head,
          carbonLoadModel,
          segmentIdToLoadStartTimeMapping.get(validSegments.head).get)
      } catch {
        case ex: Exception =>
          throw ex
      }
    }
  }
}
