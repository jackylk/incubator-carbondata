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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AlterTableDropColumnModel
import org.apache.spark.sql.hive.CarbonInternalMetastore
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{AlterTableDropColumnPreEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class AlterTableDropColumnEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableDropColumnPreEvent: AlterTableDropColumnPreEvent =>
        LOGGER.info("alter table drop column event listener called")
        val carbonTable = alterTableDropColumnPreEvent.carbonTable
        val dbName = carbonTable.getDatabaseName
        val tableName = carbonTable.getTableName
        val tablePath = carbonTable.getTablePath
        val sparkSession = alterTableDropColumnPreEvent.sparkSession
        val alterTableDropColumnModel = alterTableDropColumnPreEvent.alterTableDropColumnModel
        dropApplicableSITables(dbName,
          tableName,
          tablePath,
          alterTableDropColumnModel)(sparkSession)
    }
  }

  private def dropApplicableSITables(dbName: String,
      tableName: String,
      tablePath: String,
      alterTableDropColumnModel: AlterTableDropColumnModel)
    (sparkSession: SparkSession) = {
    var indexTableToDrop: Seq[String] = Seq.empty
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val parentCarbonTable = catalog.lookupRelation(Some(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    CarbonInternalScalaUtil.getIndexesMap(parentCarbonTable).asScala
      .foreach(indexTable => {
        var colSize = 0
        indexTable._2.asScala.foreach(column =>
          if (alterTableDropColumnModel.columns.contains(column)) {
            colSize += 1
          })
        if (colSize > 0) {
          if (colSize == indexTable._2.size) {
            indexTableToDrop ++= Seq(indexTable._1)
          } else {
            sys
              .error(s"Index Table [${
                indexTable
                  ._1
              }] exists with combination of provided drop column(s) and other columns, drop " +
                     s"index table & retry")
          }
        }
      })
    indexTableToDrop.foreach { indexTable =>
      val indexCarbonTable = catalog.getTableFromMetadataCache(dbName, indexTable).orNull
      CarbonInternalMetastore.refreshIndexInfo(dbName, indexTable, indexCarbonTable)(sparkSession)
      val indexTableIdentifier = TableIdentifier(indexTable, Some(dbName))
      // drop carbon table
      CarbonInternalMetastore
        .dropIndexTable(indexTableIdentifier,
          indexCarbonTable,
          tablePath,
          parentCarbonTable,
          removeEntryFromParentTable = true)(sparkSession)
    }
  }
}
