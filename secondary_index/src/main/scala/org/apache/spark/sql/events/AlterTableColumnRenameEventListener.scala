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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.execution.command.AlterTableDataTypeChangeModel
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableColRenameDataTypeChangeCommand
import org.apache.spark.sql.hive.CarbonInternalHiveMetadataUtil.refreshTable
import org.apache.spark.util.{AlterTableUtil, CarbonInternalScalaUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.events._
import org.apache.carbondata.events.exception.PostEventException
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.spark.indextable.{IndexTableInfo, IndexTableUtil}

/**
 * Listener class to rename the column present in index tables
 */
class AlterTableColumnRenameEventListener extends OperationEventListener with Logging {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event,
    operationContext: OperationContext): Unit = {
    event match {
      case alterTableColRenameAndDataTypeChangePreEvent
        : AlterTableColRenameAndDataTypeChangePreEvent =>
        val carbonTable = alterTableColRenameAndDataTypeChangePreEvent.carbonTable
        // direct column rename on index table is not allowed
        if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
          if (!operationContext.getProperty("childTableColumnRename").toString.toBoolean) {
            throw new MalformedCarbonCommandException(
              "Alter table column rename is not allowed on index table.")
          }
        }
      case alterTableColRenameAndDataTypeChangePostEvent
        : AlterTableColRenameAndDataTypeChangePostEvent
        if alterTableColRenameAndDataTypeChangePostEvent
          .alterTableDataTypeChangeModel.isColumnRename =>
        val alterTableDataTypeChangeModel = alterTableColRenameAndDataTypeChangePostEvent
          .alterTableDataTypeChangeModel
        val sparkSession = alterTableColRenameAndDataTypeChangePostEvent.sparkSession
        val databaseName = alterTableDataTypeChangeModel.databaseName
        val carbonTable = alterTableColRenameAndDataTypeChangePostEvent.carbonTable
        val catalog = CarbonEnv
          .getInstance(alterTableColRenameAndDataTypeChangePostEvent.sparkSession).carbonMetaStore
        val newColumnName = alterTableDataTypeChangeModel.newColumnName
        val oldColumnName = alterTableDataTypeChangeModel.columnName
        val dataTypeInfo = alterTableDataTypeChangeModel.dataTypeInfo
        val carbonColumns = carbonTable
          .getCreateOrderColumn(alterTableDataTypeChangeModel.tableName).asScala
          .filter(!_.isInvisible)
        val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(oldColumnName))
        var indexTablesToRenameColumn: Seq[String] = Seq.empty
        CarbonInternalScalaUtil.getIndexesMap(carbonTable).asScala.foreach(
          indexTable =>
            indexTable._2.asScala.foreach(column =>
              if (oldColumnName.equalsIgnoreCase(column)) {
                indexTablesToRenameColumn ++= Seq(indexTable._1)
              }))
        val indexTablesRenamedSuccess = indexTablesToRenameColumn
          .takeWhile { indexTable =>
            val alterTableColRenameAndDataTypeChangeModel =
              AlterTableDataTypeChangeModel(
                dataTypeInfo,
                databaseName,
                indexTable,
                oldColumnName,
                newColumnName,
                alterTableDataTypeChangeModel.isColumnRename
              )
            // Fire CarbonAlterTableColRenameDataTypeChangeCommand for each index tables
            try {
              CarbonAlterTableColRenameDataTypeChangeCommand(
                alterTableColRenameAndDataTypeChangeModel, childTableColumnRename = true)
                .run(alterTableColRenameAndDataTypeChangePostEvent.sparkSession)
              LOGGER
                .info(s"Column rename for index $indexTable is successful. Index column " +
                      s"$oldColumnName is successfully renamed to $newColumnName")
              true
            } catch {
              case ex: Exception =>
                LOGGER
                  .error(
                    "column rename is failed for index table, reverting the changes for all the " +
                    "successfully renamed index tables.",
                    ex)
                false
            }
          }
        // if number of successful index table column rename should be equal to total index tables
        // to rename column, else revert the successful ones
        val needRevert = indexTablesToRenameColumn.length != indexTablesRenamedSuccess.length
        if (needRevert) {
          indexTablesRenamedSuccess.foreach { indexTable =>
            val indexCarbonTable = catalog.getTableFromMetadataCache(databaseName.get, indexTable)
              .orNull
            if (indexCarbonTable != null) {
              // failure tables will be automatically taken care in
              // CarbonAlterTableColRenameDataTypeChangeCommand, just need to revert the success
              // tables, so get the latest timestamp for evolutionhistory
              val thriftTable: TableInfo = catalog.getThriftTableInfo(indexCarbonTable)
              val evolutionEntryList = thriftTable.fact_table.schema_evolution
                .schema_evolution_history
              AlterTableUtil
                .revertColumnRenameAndDataTypeChanges(indexCarbonTable.getDatabaseName,
                  indexCarbonTable.getTableName,
                  evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp)(
                  alterTableColRenameAndDataTypeChangePostEvent.sparkSession)
            }
          }
          throw PostEventException("Alter table column rename failed for index tables")
        } else {
          val database = sparkSession.catalog.currentDatabase
          // set the new indexInfo after column rename
          indexTablesRenamedSuccess.foreach { indexTable =>
            val indexCarbonTable = catalog.
              getTableFromMetadataCache(databaseName.getOrElse(database), indexTable).orNull
            val indexTableCols: java.util.List[String] = new util.ArrayList[String]()
            val oldIndexInfo = CarbonInternalScalaUtil.getIndexInfo(carbonTable)
            indexCarbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.foreach { column =>
              if (!column.getColumnName
                .equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_REFERENCE) &&
                  !column.getColumnName
                    .equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)) {
                indexTableCols.add(column.getColumnName)
              }
            }
            val indexInfo = IndexTableUtil.checkAndAddIndexTable(oldIndexInfo,
              new IndexTableInfo(database, indexTable,
                indexTableCols))
            sparkSession.sql(
              s"""ALTER TABLE $database.${carbonTable.getTableName}
          SET SERDEPROPERTIES ('indexInfo' = '$indexInfo')""".stripMargin)
          }
          refreshTable(database, carbonTable.getTableName, sparkSession)
        }
      case _ =>
    }
  }
}
