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

import org.apache.spark.sql.CarbonDatasourceHadoopRelation

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.spark.core.metadata.IndexMetadata
import org.apache.carbondata.spark.spark.indextable.{IndexTableInfo, IndexTableUtil}

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

  def getIndexInfo(relation: CarbonDatasourceHadoopRelation): String = {
    if (!isIndexTable(relation.carbonTable)) {
      val databaseName = relation.carbonRelation.carbonTable.getCarbonTableIdentifier
        .getDatabaseName
      val carbonTable = relation.carbonRelation.carbonTable
      IndexTableUtil.toGson(CarbonInternalScalaUtil.getIndexesMap(carbonTable).asScala.map(
        entry => new IndexTableInfo(databaseName, entry._1, entry._2)).toArray)
    } else {
      IndexTableUtil.toGson(new Array[IndexTableInfo](0))
    }
  }

  def getIndexes(relation: CarbonDatasourceHadoopRelation): scala.collection.mutable.Map[String,
    Array[String]] = {
    val indexes = scala.collection.mutable.Map[String, Array[String]]()
    IndexTableUtil.fromGson(getIndexInfo(relation)).foreach { indexTableInfo =>
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
      .readLoadMetadata(indexTable.getMetaDataFilepath)
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
}
