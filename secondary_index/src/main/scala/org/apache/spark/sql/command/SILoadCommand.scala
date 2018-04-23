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

package org.apache.spark.sql.command

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus,
SegmentStatusManager}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.rdd.SecondaryIndexCreator
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil

case class SecondaryIndex(var databaseName: Option[String], tableName: String,
    columnNames: List[String], indexTableName: String)

case class SecondaryIndexModel(sqlContext: SQLContext,
    carbonLoadModel: CarbonLoadModel,
    carbonTable: CarbonTable,
    secondaryIndex: SecondaryIndex,
    validSegments: List[String],
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long])

/**
 * Runnable Command for creating secondary index for the specified columns
 *
 * @param indexModel
 */
private[sql] case class LoadDataForSecondaryIndex(indexModel: SecondaryIndex) extends
  RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = indexModel.tableName
    val databaseName = CarbonEnv.getDatabaseName(indexModel.databaseName)(sparkSession)
    val relation =
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(indexModel.databaseName, tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    // get table metadata, alter table and delete segment lock because when secondary index
    // creation is in progress no other modification is allowed for the same table
    try {
      val carbonLoadModel = new CarbonLoadModel()
      val table = relation.carbonTable
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(relation.carbonTable.getTableName)
      carbonLoadModel.setDatabaseName(relation.carbonTable.getDatabaseName)
      carbonLoadModel.setTablePath(relation.carbonTable.getTablePath)
      createSecondaryIndex(sparkSession, indexModel, carbonLoadModel)
    } catch {
      case ex: Exception =>
        throw ex
    }
    Seq.empty
  }

  def createSecondaryIndex(sparkSession: SparkSession,
      secondaryIndex: SecondaryIndex,
      carbonLoadModel: CarbonLoadModel): Unit = {
    var details: Array[LoadMetadataDetails] = null
    // read table status file to validate for no load scenario and get valid segments
    if (null == carbonLoadModel.getLoadMetadataDetails) {
      details = readTableStatusFile(carbonLoadModel)
      carbonLoadModel.setLoadMetadataDetails(details.toList.asJava)
    }
    if (!carbonLoadModel.getLoadMetadataDetails.isEmpty) {
      try {
        // get list of valid segments for which secondary index need to be created
        val validSegments = CarbonInternalLoaderUtil.getListOfValidSlices(details).asScala.toList
        if (validSegments.nonEmpty) {
          val segmentIdToLoadStartTimeMapping:
            scala.collection.mutable.Map[String, java.lang.Long] =
            CarbonInternalLoaderUtil.getSegmentToLoadStartTimeMapping(details).asScala
          val secondaryIndexModel = SecondaryIndexModel(sparkSession.sqlContext, carbonLoadModel,
            carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
            secondaryIndex, validSegments, segmentIdToLoadStartTimeMapping)
          SecondaryIndexCreator.createSecondaryIndex(secondaryIndexModel)
          val indexTableMeta = CarbonEnv.getInstance(sparkSession).carbonMetastore
            .getTableFromMetadataCache(secondaryIndexModel.carbonLoadModel.getDatabaseName,
              secondaryIndexModel.secondaryIndex.indexTableName).getOrElse(null)
          val indexCarbonTable = if (null != indexTableMeta) {
            indexTableMeta
          } else {
            null
          }
          if (null == indexCarbonTable) {
            throw new Exception("Not able to load Index carbon table from metadata table cache")
          }
          val tableStatusUpdation = FileInternalUtil.updateTableStatus(
            secondaryIndexModel.validSegments,
            secondaryIndexModel.carbonLoadModel.getDatabaseName,
            secondaryIndexModel.secondaryIndex.indexTableName,
            SegmentStatus.SUCCESS,
            secondaryIndexModel.segmentIdToLoadStartTimeMapping,
            indexCarbonTable)
          if (!tableStatusUpdation) {
            throw new Exception("Table status updation failed while creating secondary index")
          }
        }
      } catch {
        case ex: Exception =>
          throw ex
      }
    }

    def readTableStatusFile(model: CarbonLoadModel): Array[LoadMetadataDetails] = {
      val metadataPath = model.getCarbonDataLoadSchema.getCarbonTable.getMetadataPath
      val details = SegmentStatusManager.readLoadMetadata(metadataPath)
      details
    }
  }
}
