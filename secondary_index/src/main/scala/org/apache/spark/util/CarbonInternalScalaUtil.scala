/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.util

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.command.{SecondaryIndex, SecondaryIndexModel}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.core.metadata.IndexMetadata
import org.apache.carbondata.spark.rdd.SecondaryIndexCreator
import org.apache.carbondata.spark.spark.indextable.{IndexTableInfo, IndexTableUtil}
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil

/**
 *
 */
object CarbonInternalScalaUtil {

  def addIndexTableInfo(carbonTable: CarbonTable,
      tableName: String,
      columns: java.util.List[String]): Unit = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).addIndexTableInfo(tableName, columns)
    }
  }

  def removeIndexTableInfo(carbonTable: CarbonTable, tableName: String): Unit = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).removeIndexTableInfo(tableName)
    }
  }

  def getIndexesMap(carbonTable: CarbonTable): java.util.Map[String, java.util.List[String]] = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    val indexesMap = if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).getIndexesMap
    } else {
      new util.HashMap[String, util.List[String]]()
    }
    indexesMap
  }

  def getIndexesTables(carbonTable: CarbonTable): java.util.List[String] = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    val indexesTables = if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).getIndexTables
    } else {
      new java.util.ArrayList[String]
    }
    indexesTables
  }

  def isIndexTable(carbonTable: CarbonTable): Boolean = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    val isIndexesTables = if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).isIndexTable
    } else {
      false
    }
    isIndexesTables
  }

  def getParentTableName(carbonTable: CarbonTable): String = {
    val indexMeta = carbonTable.getTableInfo.getFactTable.getTableProperties
      .get(carbonTable.getCarbonTableIdentifier.getTableId)
    val indexesTables = if (null != indexMeta) {
      IndexMetadata.deserialize(indexMeta).getParentTableName
    } else {
      null
    }
    indexesTables
  }

  def getIndexInfo(carbonTable: CarbonTable): String = {
    if (!isIndexTable(carbonTable)) {
      IndexTableUtil.toGson(CarbonInternalScalaUtil.getIndexesMap(carbonTable).asScala.map(
        entry => new IndexTableInfo(carbonTable.getDatabaseName, entry._1, entry._2)).toArray)
    } else {
      IndexTableUtil.toGson(new Array[IndexTableInfo](0))
    }
  }

  def getIndexes(relation: CarbonDatasourceHadoopRelation): scala.collection.mutable.Map[String,
    Array[String]] = {
    val indexes = scala.collection.mutable.Map[String, Array[String]]()
    val carbonTable = relation.carbonRelation.carbonTable
    IndexTableUtil.fromGson(getIndexInfo(carbonTable)).foreach { indexTableInfo =>
      indexes.put(indexTableInfo.getTableName, indexTableInfo.getIndexCols.asScala.toArray)
    }
    indexes
  }

  /**
   * For a given index table this method will prepare the table status details
   *
   * @param factLoadMetadataDetails
   * @param indexTable
   * @param newSegmentDetailsObject
   * @return
   */
  def getTableStatusDetailsForIndexTable(factLoadMetadataDetails: util.List[LoadMetadataDetails],
      indexTable: CarbonTable,
      newSegmentDetailsObject: util.List[LoadMetadataDetails]): util.List[LoadMetadataDetails] = {
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(indexTable
      .getAbsoluteTableIdentifier)
    val indexTableDetailsList: util.List[LoadMetadataDetails] = new util
    .ArrayList[LoadMetadataDetails](
      factLoadMetadataDetails.size)
    val indexTableStatusDetailsArray: Array[LoadMetadataDetails] = SegmentStatusManager
      .readLoadMetadata(indexTable.getMetadataPath)
    if (null !=
        indexTableStatusDetailsArray) {
      for (loadMetadataDetails <- indexTableStatusDetailsArray) {
        indexTableDetailsList.add(loadMetadataDetails)
      }
    }
    indexTableDetailsList.addAll(newSegmentDetailsObject)
    val iterator: util.Iterator[LoadMetadataDetails] = indexTableDetailsList.iterator
    // synchronize the index table status file with its parent table
    while ( { iterator.hasNext }) {
      val indexTableDetails: LoadMetadataDetails = iterator.next
      var found: Boolean = false
      for (factTableDetails <- factLoadMetadataDetails.asScala) {
        // null check is added because in case of auto load, load end time will be null
        // for the last entry
        if (0L != factTableDetails.getLoadEndTime &&
            indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(factTableDetails.getLoadStatus)
          indexTableDetails.setMajorCompacted(factTableDetails.isMajorCompacted)
          indexTableDetails.setMergedLoadName(factTableDetails.getMergedLoadName)
          indexTableDetails.setPartitionCount(factTableDetails.getPartitionCount)
          indexTableDetails
            .setModificationOrdeletionTimesStamp(factTableDetails
              .getModificationOrdeletionTimesStamp)
          indexTableDetails.setLoadEndTime(factTableDetails.getLoadEndTime)
          indexTableDetails.setVisibility(factTableDetails.getVisibility)
          found = true
          // TODO: make it breakable
        } else if (indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(CarbonCommonConstants
          // .STORE_LOADSTATUS_SUCCESS)
          indexTableDetails.setLoadEndTime(CarbonUpdateUtil.readCurrentTime)
          found = true
          // TODO: make it breakable
        }
      }
      // in case there is some inconsistency between fact table index file and index table
      // status file, it can resolved here by removing unwanted segments
      if (!found) {
        iterator.remove()
      }
    }
    indexTableDetailsList
  }

  def checkIsIndexTable(plan: LogicalPlan): Boolean = {
    plan match {
      case Aggregate(_, _, plan) if (isIndexTablesJoin(plan)) => true
      case _ => false
    }
  }

  /**
   * Collect all logical relation and check for if plan contains index table join
   *
   * @param plan
   * @return false if there are no index tables found in the plan or if logical relation is empty.
   */
  def isIndexTablesJoin(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.nonEmpty && !allRelations.exists(x =>
      !(x.relation.isInstanceOf[CarbonDatasourceHadoopRelation]
        && CarbonInternalScalaUtil
          .isIndexTable(x.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable)))
  }

  /**
   * Get the column compressor for the index table. Check first in the index table tableproperties
   * and then fall back to main table at last to the default compressor
   * @param databaseName
   * @param indexTableName
   * @param mainTableName
   * @param sparkSession
   * @return
   */
  def getCompressorForIndexTable(databaseName: String,
    indexTableName: String,
    mainTableName: String)
    (sparkSession: SparkSession): String = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(Some(databaseName), mainTableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val indexTableRelation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(Some(databaseName), indexTableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    // get the compressor from the index table (table properties)
    var columnCompressor: String = indexTableRelation.carbonTable.getTableInfo.getFactTable
      .getTableProperties.get(CarbonCommonConstants.COMPRESSOR)
    if (null == columnCompressor) {
      // if nothing is set to index table then fall to the main table compressor
      columnCompressor = relation.carbonTable.getTableInfo.getFactTable
        .getTableProperties
        .get(CarbonCommonConstants.COMPRESSOR)
      if (null == columnCompressor) {
        // if main table compressor is also not set then choose the default compressor
        columnCompressor = CompressorFactory.getInstance.getCompressor.getName
      }
    }
    columnCompressor
  }

  def getIndexCarbonTable(databaseName: String, indexTableName: String)
    (sparkSession: SparkSession): CarbonTable = {
    CarbonEnv.getCarbonTable(Some(databaseName), indexTableName)(sparkSession)
  }

  def getIndexCarbonTables(carbonTable: CarbonTable,
      sparkSession: SparkSession): util.ArrayList[CarbonTable] = {
    val indexTableNames: util.List[String] = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
    val indexTables = new util.ArrayList[CarbonTable]()
    for (indexTableName <- indexTableNames.asScala) {
      indexTables
        .add(CarbonInternalScalaUtil
          .getIndexCarbonTable(carbonTable.getDatabaseName, indexTableName)(
            sparkSession))
    }
    indexTables
  }

  /**
   * This method loads data to SI table, if isLoadToFailedSISegments is true, then load to only
   * failed segments, if false, just load the data to current segment of main table load
   */
  def LoadToSITable(sparkSession: SparkSession,
    carbonLoadModel: CarbonLoadModel,
    indexTableName: String,
    isLoadToFailedSISegments: Boolean,
    secondaryIndex: SecondaryIndex,
    carbonTable: CarbonTable): Unit = {

    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    var indexTable = metaStore
      .lookupRelation(Some(carbonLoadModel.getDatabaseName),
        indexTableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable

    val details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)

    var segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
      scala.collection.mutable.Map()

    val segmentsToReload: scala.collection.mutable.ListBuffer[String] = scala
      .collection
      .mutable.ListBuffer[String]()

    if (isLoadToFailedSISegments) {
      // read the details of SI table and get all the failed segments during SI creation which are
      // MARKED_FOR_DELETE
      details.collect {
        case loadMetaDetail: LoadMetadataDetails =>
          if (loadMetaDetail.getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE) {
            segmentsToReload.append(loadMetaDetail.getLoadName)
          }
      }
      segmentIdToLoadStartTimeMapping = CarbonInternalLoaderUtil
        .getSegmentToLoadStartTimeMapping(carbonLoadModel.getLoadMetadataDetails.asScala.toArray)
        .asScala
    } else {
      segmentIdToLoadStartTimeMapping = scala.collection.mutable
        .Map((carbonLoadModel.getSegmentId, carbonLoadModel.getFactTimeStamp))
    }
    val secondaryIndexModel = if (isLoadToFailedSISegments) {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        segmentsToReload.toList,
        segmentIdToLoadStartTimeMapping)
    } else {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        List(carbonLoadModel.getSegmentId),
        segmentIdToLoadStartTimeMapping)
    }

    val segmentToSegmentTimestampMap: java.util.Map[String, String] = new java.util
    .HashMap[String, String]()
    val indexCarbonTable = SecondaryIndexCreator
      .createSecondaryIndex(secondaryIndexModel,
        segmentToSegmentTimestampMap,
        indexTable,
        forceAccessSegment = true,
        isCompactionCall = false,
        isLoadToFailedSISegments)
  }

}
