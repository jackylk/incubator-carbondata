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

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.CarbonInternalMetastore
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath

/**
 * Command to drop secondary index on a table
 */
private[sql] case class DropIndex(ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    parentTableName: String = null, var dropIndexSql: String = null)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    var tableIdentifierForAcquiringLock = carbonTableIdentifier
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val databaseLoc = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    // flag to check if folders and files can be successfully deleted
    var isValidDeletion = false
    val carbonLocks: scala.collection.mutable.ArrayBuffer[ICarbonLock] = ArrayBuffer[ICarbonLock]()
    try {
      catalog.checkSchemasModifiedTimeAndReloadTables()
      val carbonTable: Option[CarbonTable] =
        catalog.getTableFromMetadataCache(dbName, tableName) match {
          case Some(carbonTable) => Some(carbonTable)
          case None => try {
            Some(catalog.lookupRelation(Some(dbName), tableName)(sparkSession)
              .asInstanceOf[CarbonRelation].metaData.carbonTable)
          } catch {
            case ex: NoSuchTableException =>
              if (!ifExistsSet) {
                throw ex
              }
              None
          }
        }

      if (carbonTable.isDefined) {
        CarbonInternalMetastore.refreshIndexInfo(dbName, tableName, carbonTable.get)(sparkSession)
        val isIndexTableBool = CarbonInternalScalaUtil.isIndexTable(carbonTable.get)
        val parentTableName = CarbonInternalScalaUtil.getParentTableName(carbonTable.get)
        if (!isIndexTableBool) {
          sys.error(s"Drop Index command is not permitted on carbon table [$dbName.$tableName]")
        } else if (isIndexTableBool &&
                   !parentTableName.equalsIgnoreCase(parentTableName)) {
          sys.error(s"Index Table [$dbName.$tableName] does not exist on " +
                    s"parent table [$dbName.$parentTableName]")
        } else {
          if (isIndexTableBool) {
            tableIdentifierForAcquiringLock = new CarbonTableIdentifier(dbName,
              parentTableName, "")
          }
          locksToBeAcquired foreach {
            lock => {
              val tableIdentifier =
                AbsoluteTableIdentifier
                  .from(carbonTable.get.getTablePath, dbName.toLowerCase, tableName.toLowerCase)
              carbonLocks +=
              CarbonLockUtil
                .getLockObject(tableIdentifier, lock)
            }
          }
          isValidDeletion = true
        }

        val tableIdentifier = TableIdentifier(tableName, Some(dbName))
        // drop carbon table
        val parentCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Some(dbName), parentTableName)(sparkSession).asInstanceOf[CarbonRelation]
          .carbonTable
        val tablePath = carbonTable.get.getTablePath

        CarbonInternalMetastore.dropIndexTable(tableIdentifier, carbonTable.get,
          tablePath,
          parentCarbonTable,
          removeEntryFromParentTable = true)(sparkSession)
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(ex, s"Dropping table $dbName.$tableName failed")
        if (!ifExistsSet) {
          sys.error(s"Dropping table $dbName.$tableName failed: ${ ex.getMessage }")
        }
    } finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          logInfo("Table MetaData Unlocked Successfully")
          if (isValidDeletion) {
            val tablePath = databaseLoc + CarbonCommonConstants.FILE_SEPARATOR + tableName
            // deleting any remaining files.
            val metadataFilePath = CarbonStorePath
              .getCarbonTablePath(tablePath, carbonTableIdentifier).getMetadataDirectoryPath
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
    Seq.empty
  }
}
