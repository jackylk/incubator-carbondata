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
package org.apache.spark.sql.acl

import java.io.File
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.acl.InternalCarbonConstant
import org.apache.carbondata.spark.acl.filesystem.PrivilegedFileOperation


object Utils {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def proxyOperate(
      loginUser: UserGroupInformation,
      currentUser: UserGroupInformation,
      msg: String,
      doOriLogic: Boolean = true)(f: => Unit): Unit = {
    if (new SparkConf().getBoolean("spark.isThriftServer", false) &&
        loginUser.getShortUserName != currentUser.getShortUserName) {
      LOGGER.info(msg)
      loginUser.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          f
        }
      })
    } else {
      if (doOriLogic) {
        f
      }
    }
  }

  /**
   * This method will create a list of all index table paths for applying permissions on the
   * folder in HDFS
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  def getIndexTablePathList(databaseName: String, tableName: String,
      carbonTable: CarbonTable): List[String] = {
    List[String]()
  }

//  val indexTables = carbonTable.getIndexTables
//  val indexTables = new util.ArrayList[String]()
//  var indexTablePathList = List[CarbonTablePath]()
//  indexTables.foreach { indexTableName =>
//    val indexCarbonTable = org.apache.carbondata.core.metadata.CarbonMetadata
//      .getInstance().getCarbonTable(databaseName + '_' + indexTableName)
//    val indexTablePath = CarbonStorePath.getCarbonTablePath(
//      indexCarbonTable.getStorePath,
//      indexCarbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier)
//    indexTablePathList :+= indexTablePath
//  }
//  indexTablePathList
// }

  def initCarbonFoldersPermission(storeLocation: String, sparkSession: SparkSession): Unit = {
    CarbonProperties.getInstance().addProperty(
      InternalCarbonConstant.CARBON_ACCESS_CONTROL_RULE_ENABLED,
      if (ACLFileUtils.isSecureModeEnabled()) "true" else "false");
    PrivilegedFileOperation.execute(new PrivilegedExceptionAction[Unit] {
      override def run: Unit = {
        // ignore exception while store location creation
        // and default database folder creation while startup
        try {
          FileFactory.createDirectoryAndSetPermission(storeLocation,
            ACLFileUtils.getPermissionsOnDatabase()
          )
          FileFactory.createDirectoryAndSetPermission(storeLocation + File.separator + "default",
            ACLFileUtils.getPermissionsOnDatabase()
          )
        } catch {
          case e : Exception =>
            LOGGER
              .warn(s"Ignore exception occurred while carbon env initialization: ${e.getMessage}")
        }
      }
    })
  }
}
