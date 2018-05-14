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
