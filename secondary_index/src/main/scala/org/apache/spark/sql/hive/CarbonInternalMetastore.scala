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

package org.apache.spark.sql.hive

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.spark.core.metadata.IndexMetadata
import org.apache.carbondata.spark.spark.indextable.{IndexTableInfo, IndexTableUtil}

object CarbonInternalMetastore {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * This method can be used to delete the index table and update the parent table and hive
   * metadata.
   *
   * @param indexCarbonTable
   * @param tableStorePath
   * @param parentCarbonTable
   * @param removeEntryFromParentTable if true then the index table info would be removed from
   *                                   the Parent table
   * @param sparkSession
   */
  def dropIndexTable(indexTableIdentifier: TableIdentifier, indexCarbonTable: CarbonTable,
      tableStorePath: String,
      parentCarbonTable: CarbonTable,
      removeEntryFromParentTable: Boolean)(sparkSession: SparkSession) {
    val dbName = indexTableIdentifier.database.get
    val tableName = indexTableIdentifier.table
    try {
      if (indexCarbonTable != null) {
        ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(indexCarbonTable)
        LOGGER.audit(s"Deleting index table $dbName.$tableName")
        deleteTableDirectory(indexCarbonTable.getCarbonTableIdentifier, sparkSession)
        if (removeEntryFromParentTable && parentCarbonTable != null) {
          val parentTableName = parentCarbonTable.getTableName
          val relation = sparkSession.sessionState.catalog
            .lookupRelation(TableIdentifier(parentTableName, Some(dbName)))
          val indexInfo = if (relation != null) {
            val datasourceHadoopRelation = CarbonInternalHiveMetadataUtil.retrieveRelation(relation)
            CarbonInternalScalaUtil.getIndexInfo(datasourceHadoopRelation)
          } else {
            sys.error(s"Table $dbName.$parentTableName does not exists")
          }
          sparkSession.sessionState.catalog.dropTable(indexTableIdentifier, true, false)
          // even if folders are deleted from carbon store it can happen that table exists in hive
          CarbonInternalHiveMetadataUtil
            .invalidateAndUpdateIndexInfo(indexTableIdentifier, indexInfo, parentCarbonTable)(
              sparkSession)
        }
      }
    } finally {
      // Even if some exception occurs we will try to remove the table from catalog to avoid
      // stale state.
      sparkSession.sessionState.catalog
        .dropTable(indexTableIdentifier, ignoreIfNotExists = true, purge = false)
      sparkSession.sessionState.catalog.refreshTable(indexTableIdentifier)
      LOGGER.audit(s"Deleted index table $dbName.$tableName")
    }
  }

  /**
   * This method will delete the index tables silently. We want this because even if one index
   * delete fails, we need to try delete on all other index tables as well.
   *
   * @param carbonTableIdentifier
   * @param storePath
   * @param sparkSession
   */
  def deleteIndexSilent(carbonTableIdentifier: TableIdentifier, storePath: String)
    (sparkSession: SparkSession): Unit = {
    val dbName = carbonTableIdentifier.database
    val indexTable = carbonTableIdentifier.table
    var indexCarbonTable: CarbonTable = null
    try {
      indexCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(dbName, indexTable)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable
    } catch {
      case e: Exception =>
        LOGGER.error("Exception occurred while drop index table for : " +
                     s"$dbName.$indexTable : ${ e.getMessage }")
    }
    finally {
      try {
        dropIndexTable(carbonTableIdentifier,
          indexCarbonTable,
          storePath,
          null,
          removeEntryFromParentTable = false
        )(sparkSession)
      } catch {
        case e: Exception =>
          LOGGER.error("Exception occurred while drop index table for : " +
                       s"$dbName.$indexTable : ${ e.getMessage }")
      }
    }
  }

  def refreshIndexInfo(dbName: String, tableName: String,
      carbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    // When Index information is not loaded in main table, then it will fetch
    // index info from hivemetastore and set it in the carbon table.
    if (null != carbonTable) {
      val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
      if (indexesMap == null || indexesMap.isEmpty) {
        val indexTableMap = new ConcurrentHashMap[String, java.util.List[String]]
        try {
          val (isIndexTable, parentTableName, indexInfo, parentTablePath, parentTableId) =
            indexInfoFromHive(dbName, tableName)(sparkSession)
          if (isIndexTable.equals("true")) {
            val indexMeta = new IndexMetadata(indexTableMap,
              parentTableName,
              true,
              parentTablePath,
              parentTableId)
            setDictionaryPathInCarbonTable(carbonTable, indexMeta)
            carbonTable.getTableInfo.getFactTable.getTableProperties
              .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMeta.serialize)
          } else {
            IndexTableUtil.fromGson(indexInfo)
              .foreach { indexTableInfo =>
                indexTableMap
                  .put(indexTableInfo.getTableName, indexTableInfo.getIndexCols)
              }
            val indexMetadata = new IndexMetadata(indexTableMap,
                parentTableName,
                isIndexTable.toBoolean,
                parentTablePath, parentTableId)
            carbonTable.getTableInfo.getFactTable.getTableProperties
              .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMetadata.serialize)
          }
        } catch {
          case e: Exception =>
            // In case of creating a table, hivetable will not be available.
            LOGGER.error(e, e.getMessage)
        }
      }
    }
  }

  /**
   * set dictionary location in carbon table
   *
   * @param carbonTable
   * @param indexMetadata
   */
  private def setDictionaryPathInCarbonTable(carbonTable: CarbonTable,
      indexMetadata: IndexMetadata): Unit = {
    val indexTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val newTablePath: String = CarbonUtil
      .getNewTablePath(indexTablePath, indexMetadata.getParentTableName)
    val parentTableAbsoluteTableIdentifier = AbsoluteTableIdentifier
      .from(newTablePath,
        carbonTable.getDatabaseName,
        indexMetadata.getParentTableName,
        indexMetadata.getParentTableId)
    val parentCarbonTablePath = CarbonStorePath
      .getCarbonTablePath(parentTableAbsoluteTableIdentifier)
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .put(CarbonCommonConstants.DICTIONARY_PATH, parentCarbonTablePath.getMetadataDirectoryPath)
  }

  private def indexInfoFromHive(databaseName: String, tableName: String)
    (sparkSession: SparkSession): (String, String, String, String, String) = {
    val hiveTable = sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
    val indexList = hiveTable.storage.properties.getOrElse(
      "indexInfo", IndexTableUtil.toGson(new Array[IndexTableInfo](0)))

    val datasourceOptions = optionsValueFromParts(hiveTable)

    val isIndexTable = datasourceOptions.getOrElse("isIndexTable", "false")
    val parentTableName = datasourceOptions.getOrElse("parentTableName", "")
    val parentTablePath = datasourceOptions.getOrElse("parentTablePath", "")
    val parentTableId = datasourceOptions.getOrElse("parentTableId", "")
    (isIndexTable, parentTableName, indexList, parentTablePath, parentTableId)
  }

  private def optionsValueFromParts(table: CatalogTable): Map[String, String] = {
    val optionsCombined = new HashMap[String, String]
    val optionsKeys: Option[String] =
      table.storage.properties.get("spark.sql.sources.options.keys.numParts").map { numParts =>
        combinePartsFromSerdeProps(numParts, "spark.sql.sources.options.keys", table).mkString
      }
    optionsKeys match {
      case Some(optKeys) =>
        optKeys.split(",").foreach { optKey =>
          table.storage.properties.get(s"$optKey.numParts").map { numParts =>
            optionsCombined.put(optKey,
              combinePartsFromSerdeProps(numParts, optKey, table).mkString)
          }
        }
        optionsCombined.asScala.toMap
      case None =>
        LOGGER.warn(s"spark.sql.sources.options.keys expected, but read nothing")
        table.storage.properties
    }
  }

  private def combinePartsFromSerdeProps(numParts: String,
      key: String, table: CatalogTable): Seq[String] = {
    val keysParts = (0 until numParts.toInt).map { index =>
      val keysPart =
        table.storage.properties.get(s"$key.part.$index").orNull
      if (keysPart == null) {
        throw new AnalysisException(
          s"Could not read $key from the metastore because it is corrupted " +
          s"(missing part $index of the $key, $numParts parts are expected).")
      }
      keysPart
    }
    keysParts
  }

  private def deleteTableDirectory(carbonTableIdentifier: CarbonTableIdentifier,
      sparkSession: SparkSession): Unit = {
    val dbName = carbonTableIdentifier.getDatabaseName
    val tableName = carbonTableIdentifier.getTableName
    val databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName.toLowerCase
    val tableIdentifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableName)
    val metadataFilePath =
      CarbonStorePath.getCarbonTablePath(tableIdentifier).getMetadataDirectoryPath
    val fileType = FileFactory.getFileType(metadataFilePath)
    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .checkSchemasModifiedTimeAndReloadTables()
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
    }
  }
}
