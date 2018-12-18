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
package org.apache.spark.sql.events

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonInternalMetastore
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.events.{DropTablePreEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class SIDropEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropTablePreEvent: DropTablePreEvent =>
        LOGGER.info("drop table pre event-listener called")
        val parentCarbonTable = dropTablePreEvent.carbonTable
        try {
          val tableIdentifier = new TableIdentifier(parentCarbonTable.getTableName,
            Some(parentCarbonTable.getDatabaseName))
          val tablePath = dropTablePreEvent.carbonTable.getTablePath
          val sparkSession = dropTablePreEvent.sparkSession
          val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          var isValidDeletion = false

          CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable).asScala
            .foreach { tableName => {

              val carbonTable = metastore
                .lookupRelation(Some(parentCarbonTable.getDatabaseName),
                  tableName)(sparkSession)
                .asInstanceOf[CarbonRelation].carbonTable
              val ifExistsSet = dropTablePreEvent.ifExistsSet
              val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
              if (null != indexesMap) {
                try {
                  val indexTableIdentifier = TableIdentifier(tableName,
                    Some(parentCarbonTable.getDatabaseName))
                  CarbonInternalMetastore
                    .deleteIndexSilent(indexTableIdentifier,
                      carbonTable.getTablePath,
                      parentCarbonTable)(sparkSession)
                  isValidDeletion = true
                } catch {
                  case ex: Exception =>
                    LOGGER
                      .error(
                        s"Dropping Index table ${ tableIdentifier.database }.${
                          tableIdentifier
                            .table
                        } failed", ex)
                    if (!ifExistsSet) {
                      sys
                        .error(s"Dropping Index table ${ tableIdentifier.database }.${
                          tableIdentifier
                            .table
                        } failed: ${ ex.getMessage }")
                    }
                } finally {
                    if (isValidDeletion) {
                      val databaseLoc = CarbonEnv
                        .getDatabaseLocation(carbonTable.getDatabaseName, sparkSession)
                      val tablePath = databaseLoc + CarbonCommonConstants.FILE_SEPARATOR +
                                      tableName
                      // deleting any remaining files.
                      val metadataFilePath = carbonTable.getMetadataPath
                      val fileType = FileFactory.getFileType(metadataFilePath)
                      if (FileFactory.isFileExist(metadataFilePath, fileType)) {
                        val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
                        CarbonUtil.deleteFoldersAndFiles(file.getParentFile)
                      }
                      import org.apache.commons.io.FileUtils
                      if (FileFactory.isFileExist(tablePath, fileType)) {
                        FileUtils.deleteDirectory(new File(tablePath))
                      }
                    }
                }
              }
            }
            }
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      case _ =>
    }
  }
}
