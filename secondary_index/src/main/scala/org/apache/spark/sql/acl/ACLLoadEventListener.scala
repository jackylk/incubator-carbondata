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

import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.acl.ACLFileUtils.{getPermissionsOnTable, setACLGroupRights}
import org.apache.spark.sql.hive.CarbonInternalMetaUtil
import org.apache.spark.sql.hive.acl._

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonQueryUtil
import org.apache.carbondata.spark.acl.{CarbonUserGroupInformation, InternalCarbonConstant}


object ACLLoadEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * to create Metadata/segments/tmp folder for standard hive partition table
   *
   * @param carbonLoadModel
   * @param sparkSession
   * @return
   */
  def createPartitionDirectory(carbonLoadModel: CarbonLoadModel)
    (sparkSession: SparkSession): String = {
    val tempFolderLoc = carbonLoadModel.getSegmentId + "_" +
                        carbonLoadModel.getFactTimeStamp + ".tmp"
    val segmentFilesLocation = CarbonTablePath
      .getSegmentFilesLocation(carbonLoadModel.getCarbonDataLoadSchema
        .getCarbonTable.getTablePath)
    val writePath = segmentFilesLocation + CarbonCommonConstants.FILE_SEPARATOR + tempFolderLoc
    if (!FileFactory.isFileExist(segmentFilesLocation)) {
      createDirectoryAndSetGroupAcl(segmentFilesLocation)(sparkSession)
      createDirectoryAndSetGroupAcl(writePath)(sparkSession)
    }
    writePath
  }

  /**
   * creeate directory with 777 permission and set ACL group rights
   *
   * @param dirPath
   * @param sparkSession
   */
  def createDirectoryAndSetGroupAcl(dirPath: String)(sparkSession: SparkSession): Unit = {
    CarbonUserGroupInformation.getInstance.getCurrentUser
      .doAs(new PrivilegedExceptionAction[Unit]() {
        override def run(): Unit = {
          FileFactory.createDirectoryAndSetPermission(dirPath,
            getPermissionsOnTable())
        }
      });
    val path = new Path(dirPath)
    setACLGroupRights(CarbonUserGroupInformation.getInstance.getCurrentUser,
      path.getFileSystem(sparkSession.sqlContext.sparkContext.hadoopConfiguration), path)
  }

  class ACLPreLoadEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val loadTablePreExecutionEvent = event.asInstanceOf[LoadTablePreExecutionEvent]
      val carbonLoadModel = loadTablePreExecutionEvent.getCarbonLoadModel
      val sparkSession = loadTablePreExecutionEvent.getSparkSession
      val factPath = loadTablePreExecutionEvent.getFactPath
      val isDataFrameDefined = loadTablePreExecutionEvent.isDataFrameDefined
      val optionsFinal = loadTablePreExecutionEvent.getOptionsFinal

      if (!ACLFileUtils.isCarbonDataLoadGroupExist(sparkSession.sparkContext)) {
        val carbonDataLoadGroup = CarbonProperties.getInstance.
          getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
            InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT)
        val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser.getShortUserName
        throw PreEventException(s"CarbonDataLoad Group: $carbonDataLoadGroup is not set for the " +
                                s"user $currentUser", false)
      }
      val dbName = carbonLoadModel.getDatabaseName
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val bad_records_logger_enable = if (optionsFinal.containsKey("bad_records_logger_enable")) {
        optionsFinal.get("bad_records_logger_enable")
      } else {
        ""
      }
      val bad_records_action = if (optionsFinal.containsKey("bad_records_action")) {
        optionsFinal.get("bad_records_action")
      } else {
        ""
      }
      var bad_record_path = optionsFinal.get("bad_record_path")
      if (!StringUtils.isEmpty(bad_record_path)) {
        bad_record_path = CarbonUtil.checkAndAppendHDFSUrl(bad_record_path)
      }
      // loadPre2
      if (ACLFileUtils.isSecureModeEnabled) {
        val aclInterface: ACLInterface = CarbonInternalMetaUtil.getACLInterface(sparkSession)
        val files: java.util.List[String] =
          new java.util.ArrayList[String](CarbonCommonConstants.CONSTANT_SIZE_TEN)
        CarbonQueryUtil.splitFilePath(factPath, files, CarbonCommonConstants.COMMA)
        val objSet = files.asScala.collect {
          case filePath if ACLFileUtils.isACLSupported(filePath) =>
            new PrivObject(ObjectType.FILE, null, filePath, null, Set(PrivType.SELECT_NOGRANT))
        }.toSet
        if (!isDataFrameDefined && !aclInterface.checkPrivilege(objSet)) {
          throw PreEventException(
            "User does not have read permission for one or more csv files", false)
        }
      }

      if (ACLFileUtils.isACLSupported(bad_record_path)) {
        checkACLForBadRecordPath(bad_records_logger_enable,
          bad_records_action,
          bad_record_path)(sparkSession)
      }

      val partitionDirectoryPath = if (carbonTable.isHivePartitionTable) {
        createPartitionDirectory(carbonLoadModel)(sparkSession)
      } else {
        ""
      }


      val folderListBeforLoad = takeSnapshotBeforeLoad(sparkSession.sqlContext,
        carbonTable.getCarbonTableIdentifier,
        bad_records_logger_enable,
        bad_records_action,
        dbName,
        carbonTable,
        bad_record_path,
        partitionDirectoryPath)
      val pathArrBeforeLoadOperation = ACLFileUtils
        .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforLoad)
      operationContext
        .setProperty(ACLFileUtils.getFolderListKey(carbonTable.getCarbonTableIdentifier),
          folderListBeforLoad)
      operationContext
        .setProperty(ACLFileUtils.getPathListKey(carbonTable.getCarbonTableIdentifier),
          pathArrBeforeLoadOperation)
    }

    def checkACLForBadRecordPath(bad_records_logger_enable: String,
        bad_records_action: String,
        bad_records_path: String)
      (sparkSession: SparkSession): Unit = {
      if (bad_records_logger_enable.toBoolean ||
          LoggerAction.REDIRECT.name().equalsIgnoreCase(bad_records_action)) {

        //        if (!CarbonUtil.isValidBadStorePath(bad_record_path)) {
        //          sys.error("Invalid bad records location.")
        //        } else
        if (CarbonUtil.isValidBadStorePath(bad_records_path) && ACLFileUtils.isSecureModeEnabled) {
          val aclInterface: ACLInterface = CarbonInternalMetaUtil.getACLInterface(sparkSession)
          if (!aclInterface.checkPrivilege(Set(new PrivObject(ObjectType.FILE,
            null,
            bad_records_path,
            null,
            Set(PrivType.INSERT_NOGRANT))))) {
            throw PreEventException(
              "User does not have privileges for configured bad records folder path", false)
          }
        }
        //        bad_record_path = FileFactory
        //          .getCarbonFile(bad_record_path, FileFactory.getFileType(bad_record_path))
        //          .getCanonicalPath
      }
    }

    private def takeSnapshotBeforeLoad(sqlContext: SQLContext,
        carbonTableIdentifier: CarbonTableIdentifier,
        badRecordsLoggerEnable: String,
        badRecordsAction: String,
        dbName: String,
        carbonTable: CarbonTable,
        badRecordLocation: String,
        partitionDirectoryPath: String): List[String] = {
      var carbonBadRecordTablePath: String = null
      if (("true".equals(badRecordsLoggerEnable.toLowerCase) &&
           !LoggerAction.FAIL.name().equalsIgnoreCase(badRecordsAction)) ||
          LoggerAction.REDIRECT.name()
            .equalsIgnoreCase(badRecordsAction)) {
        carbonBadRecordTablePath = ACLFileUtils.createBadRecordsTablePath(sqlContext,
          carbonTableIdentifier, FileFactory.getUpdatedFilePath(badRecordLocation))
      }
      val tablePath = carbonTable.getTablePath
      val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
      var list = ACLFileUtils
        .getTablePathListForSnapshot(carbonTable.getMetadataPath,
          carbonTable.getPartitionInfo(carbonTable.getTableName), true)
      if (null != carbonBadRecordTablePath) {
        list = list ::: ACLFileUtils
          .getBadRecordsPathListForSnapshot(carbonBadRecordTablePath, carbonTableIdentifier)
      }
      // get the list of index tables
      val indexTablesPathList = Utils
        .getIndexTablePathList(dbName, carbonTableIdentifier.getTableName, carbonTable)
      if (!indexTablesPathList.isEmpty) {
        indexTablesPathList.foreach { indexTablePath =>
          list = list ::: ACLFileUtils.getTablePathListForSnapshot(indexTablePath,
            carbonTable.getPartitionInfo(carbonTable.getTableName))
        }
      }
      if (!partitionDirectoryPath.isEmpty) {
        list = list ::: List(partitionDirectoryPath + "/*")
      }
      list
    }
  }
  class ACLPostLoadEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      val loadTablePostExecutionEvent = event.asInstanceOf[LoadTablePostExecutionEvent]
      val carbonTable = loadTablePostExecutionEvent.getCarbonLoadModel.getCarbonDataLoadSchema
        .getCarbonTable
      ACLFileUtils
        .takeSnapAfterOperationAndApplyACL(SparkSession.getActiveSession.get,
          operationContext,
          carbonTable.getCarbonTableIdentifier,
          false,
          true
        )
    }
  }

  class ACLAbortLoadEventListener extends OperationEventListener {

    override def onEvent(event: Event,
        operationContext: OperationContext): Unit = {
      // Handle this event
    }
  }

}
