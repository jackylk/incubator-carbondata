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

package org.apache.spark.util.si

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.spark.load.CarbonInternalLoaderUtil
import org.apache.carbondata.spark.spark.util.CarbonPluginUtil


/**
 * Utility Class for the Secondary Index creation flow
 */
object FileInternalUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * This method will check and create an empty schema timestamp file
   *
   * @return
   */
  def touchStoreTimeStamp(): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType()
    val systemTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(systemTime)
    systemTime
  }

  private def getTimestampFileAndType() = {
    // if mdt file path is configured then take configured path else take default path
    val configuredMdtPath = CarbonProperties.getInstance()
      .getProperty(CarbonInternalCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonInternalCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT).trim
    var timestampFile = configuredMdtPath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    timestampFile = CarbonInternalLoaderUtil.checkAndAppendFileSystemURIScheme(timestampFile)
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  def updateTableStatus(
    validSegments: List[String],
    databaseName: String,
    tableName: String,
    loadStatus: SegmentStatus,
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
    segmentToSegmentFileNameMap: java.util.Map[String, String],
    carbonTable: CarbonTable): Boolean = {
    var loadMetadataDetailsList = Array[LoadMetadataDetails]()
    val loadEndTime = CarbonUpdateUtil.readCurrentTime
    validSegments.foreach { segmentId =>
      val loadMetadataDetail = new LoadMetadataDetails
      loadMetadataDetail.setLoadName(segmentId)
      loadMetadataDetail.setPartitionCount("0")
      loadMetadataDetail.setSegmentStatus(loadStatus)
      loadMetadataDetail.setLoadStartTime(segmentIdToLoadStartTimeMapping.get(segmentId).get)
      loadMetadataDetail.setLoadEndTime(loadEndTime)
      loadMetadataDetail.setSegmentFile(segmentToSegmentFileNameMap.get(segmentId))
      loadMetadataDetailsList +:= loadMetadataDetail
    }

    val status = CarbonInternalLoaderUtil.recordLoadMetadata(
      loadMetadataDetailsList.toList.asJava,
      validSegments.asJava,
      databaseName,
      tableName,
      carbonTable
    )
    status
  }

  /**
   * For clean up of the index table files
   *
   * @param factTable
   * @param carbonStoreLocation
   * @param isForceDeletion
   */
  def cleanIndexFiles(factTable: CarbonTable,
      carbonStoreLocation: String,
      isForceDeletion: Boolean): Unit = {
    try {
      CarbonPluginUtil
        .cleanUpIndexFiles(factTable,
          carbonStoreLocation,
          isForceDeletion)
    } catch {
      case e: Exception =>
        LOGGER
          .error("Problem cleaning up files for indexes of table : " + factTable.getTableName)
    }
  }

  def touchSchemaFileTimestamp(dbName: String,
      tableName: String,
      tablePath: String,
      schemaTimeStamp: Long): Unit = {
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName,
      tableName, UUID.randomUUID().toString)
    val tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath)
    val fileType = FileFactory.getFileType(tableMetadataFile)
    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      FileFactory.getCarbonFile(tableMetadataFile, fileType)
        .setLastModifiedTime(schemaTimeStamp)
    }
  }
}
