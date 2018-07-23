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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.acl.ACLFileUtils
import org.apache.spark.sql.hive.{CarbonInternalMetastore, CarbonRelation}
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation
import org.apache.carbondata.spark.spark.secondaryindex.SecondaryIndexUtil

/**
 * Listener for handling delete command events
 */
class DeleteFromTableEventListener extends OperationEventListener with Logging {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case deleteFromTablePreEvent: DeleteFromTablePreEvent =>
        LOGGER.audit("Delete from table pre event listener called")
        val carbonTable = deleteFromTablePreEvent.carbonTable
        // Should not allow delete on index table
        if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
          sys
            .error(s"Delete is not permitted on Index Table [${
              carbonTable
                .getDatabaseName
            }.${ carbonTable.getTableName }]")
        }
      case deleteFromTablePostEvent: DeleteFromTablePostEvent =>
        LOGGER.audit("Delete from table post event listener called")
        val parentCarbonTable = deleteFromTablePostEvent.carbonTable
        val sparkSession = deleteFromTablePostEvent.sparkSession
        CarbonInternalMetastore
          .refreshIndexInfo(parentCarbonTable.getDatabaseName,
            parentCarbonTable.getTableName,
            parentCarbonTable)(
            sparkSession)
        val indexTableList = CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable)
        if (!indexTableList.isEmpty) {
          val indexCarbonTableList = indexTableList.asScala.map { indexTableName =>
            CarbonEnv.getInstance(sparkSession).carbonMetastore
              .lookupRelation(Option(parentCarbonTable.getDatabaseName), indexTableName)(
                sparkSession)
              .asInstanceOf[CarbonRelation].carbonTable
          }.toList
          SecondaryIndexUtil
            .updateTableStatusForIndexTables(parentCarbonTable, indexCarbonTableList.asJava)
          indexCarbonTableList.foreach({ indexTable => {
            val tableStatusPath = CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath)
            val path = new Path(tableStatusPath)
            ACLFileUtils.setACLGroupRights(CarbonUserGroupInformation.getInstance.getCurrentUser,
              path.getFileSystem(sparkSession.sqlContext.sparkContext.hadoopConfiguration), path)
          }
          })
        }
    }
  }
}
