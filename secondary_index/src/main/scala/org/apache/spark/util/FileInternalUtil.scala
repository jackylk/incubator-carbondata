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
package org.apache.spark.util.si

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.util.CarbonLoaderUtil
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
    timestampFile = CarbonUtil.checkAndAppendFileSystemURIScheme(timestampFile)
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  def updateTableStatus(
    validSegments: List[String],
    databaseName: String,
    tableName: String,
    loadStatus: SegmentStatus,
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
    segmentToSegmentTimestampMap: java.util.Map[String, String],
    carbonTable: CarbonTable,
    sparkSession: SparkSession,
    newStartTime: Long = 0L,
    rebuiltSegments: Set[String] = Set.empty): Boolean = {
    var loadMetadataDetailsList = Array[LoadMetadataDetails]()
    val loadEndTime = CarbonUpdateUtil.readCurrentTime
    validSegments.foreach { segmentId =>
      val loadMetadataDetail = new LoadMetadataDetails
      loadMetadataDetail.setLoadName(segmentId)
      loadMetadataDetail.setPartitionCount("0")
      loadMetadataDetail.setSegmentStatus(loadStatus)
      if (rebuiltSegments.contains(loadMetadataDetail.getLoadName) && newStartTime != 0L) {
        loadMetadataDetail.setLoadStartTime(newStartTime)
      } else {
        loadMetadataDetail.setLoadStartTime(segmentIdToLoadStartTimeMapping(segmentId))
      }
      loadMetadataDetail.setLoadEndTime(loadEndTime)
      if (null != segmentToSegmentTimestampMap.get(segmentId)) {
        loadMetadataDetail
          .setSegmentFile(SegmentFileStore
                            .genSegmentFileName(segmentId,
                              segmentToSegmentTimestampMap.get(segmentId).toString) +
                          CarbonTablePath.SEGMENT_EXT)
      } else {
        loadMetadataDetail
          .setSegmentFile(SegmentFileStore
                            .genSegmentFileName(segmentId,
                              segmentIdToLoadStartTimeMapping.get(segmentId).get.toString) +
                          CarbonTablePath.SEGMENT_EXT)
      }
      CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(loadMetadataDetail, segmentId, carbonTable)
      loadMetadataDetailsList +:= loadMetadataDetail
    }
    val indexTables = CarbonInternalScalaUtil
      .getIndexCarbonTables(carbonTable, sparkSession)
    val status = CarbonInternalLoaderUtil.recordLoadMetadata(
      loadMetadataDetailsList.toList.asJava,
      validSegments.asJava,
      carbonTable,
      indexTables,
      databaseName,
      tableName
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
      indexTables: util.ArrayList[CarbonTable],
      carbonStoreLocation: String,
      isForceDeletion: Boolean): Unit = {
    try {
      CarbonPluginUtil
        .cleanUpIndexFiles(
          indexTables,
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
