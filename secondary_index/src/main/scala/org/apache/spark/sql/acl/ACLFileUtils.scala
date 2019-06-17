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

import java.io.FileNotFoundException
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryType, FsAction, FsPermission}
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.LockUsage
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.spark.acl.{CarbonUserGroupInformation, InternalCarbonConstant, UserGroupUtils}


object ACLFileUtils {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  def proxyOperate(
      loginUser: UserGroupInformation,
      currentUser: UserGroupInformation,
      msg: String,
      doOriLogic: Boolean = true)(f: => Unit): Unit = {
    Utils.proxyOperate(loginUser, currentUser, msg, doOriLogic)(f)
  }

  def getFolderListKey(carbonTable: CarbonTableIdentifier): String = {
    val folderListBeforeOperation = "folderListBeforeOperation"
    if (null != carbonTable) {
      (folderListBeforeOperation + '_' + carbonTable.getDatabaseName + '.' +
       carbonTable.getTableName).toLowerCase
    } else {
      // This is added for special case like MV, because identifier cannot formed in case of MV
      // because MV can be built on more than one table
      (folderListBeforeOperation + '_').toLowerCase
    }
  }

  def getPathListKey(carbonTable: CarbonTableIdentifier): String = {
    val pathArrBeforeOperation = "pathArrBeforeOperation"
    if (null != carbonTable) {
      (pathArrBeforeOperation + '_' + carbonTable.getDatabaseName + '.' +
       carbonTable.getTableName).toLowerCase
    } else {
      // This method is added for special case like MV, because identifer cannot formed in case of
      // MV. because MV can be built on more than one table
      (pathArrBeforeOperation + '_').toLowerCase
    }
  }

  /**
   * This method is added for special case like MV, because identifer cannot formed in case of MV
   * because MV can be built on more than one table
   */
  def getPathListKey: String = {
    val pathArrBeforeOperation = "pathArrBeforeOperation"
    (pathArrBeforeOperation + '_').toLowerCase
  }

  def takeRecurTraverseSnapshot(sqlContext: SQLContext,
      folderPaths: List[String], delimiter: String = "#~#",
      recursive: Boolean = false): ArrayBuffer[String] = {
    val loginUser = CarbonUserGroupInformation.getInstance.getLoginUser
    val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
    val oriPathArr = new ArrayBuffer[String]()
    Utils.proxyOperate(loginUser, currentUser,
      s"Use login user ${loginUser.getShortUserName} as a proxy user as we need " +
      s"permission to operate the given path", true) {
      var hdfs: FileSystem = null
      folderPaths.foreach { pathStr =>
        if (isACLSupported(pathStr)) {
          val path = new Path(pathStr)
          hdfs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          recurTraverse(
            hdfs,
            path,
            oriPathArr,
            loginUser.getShortUserName,
            None,
            delimiter,
            recursive
          )
        }
      }
    }
    oriPathArr
  }

  /**
   * returns the snap shot of single file or directory
   *
   * @param sqlContext
   * @param path
   * @param delimiter
   * @return
   */
  def takeNonRecursiveSnapshot(sqlContext: SQLContext,
    path: Path, delimiter: String = "#~#"): ArrayBuffer[String] = {
    val pathArray = new ArrayBuffer[String]()
    if (isACLSupported(path.toString)) {
      val loginUser = CarbonUserGroupInformation.getInstance.getLoginUser
      val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
      Utils.proxyOperate(loginUser, currentUser,
        s"Use login user ${ loginUser.getShortUserName } as a proxy user as we need " +
        s"permission to operate the given path", true) {
        val hdfs: FileSystem = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        val fileStatus = hdfs.getFileStatus(path)
        addFilePathToPathList(fileStatus.getPath, pathArray, loginUser.getShortUserName,
          delimiter, fileStatus)
      }
    }
    pathArray
  }

  def changeOwnerRecursivelyAfterOperation(isLoadOrCompaction: Boolean = false,
      sqlContext: SQLContext,
      oriPathArr: ArrayBuffer[String],
      curPathArr: ArrayBuffer[String],
      tablePath: String,
      delimiter: String = "#~#") {

      val loginUser = CarbonUserGroupInformation.getInstance.getLoginUser
      val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
      Utils.proxyOperate(loginUser, currentUser,
        s"Use login user ${loginUser.getShortUserName} as a proxy user as we need " +
        s"permission to operate the given path", true) {
        val diffPathArr = curPathArr.diff(oriPathArr)
        // Only for Load and Compaction we list for all the files of the table as we only have
        // the metadata folder path for them, rest all other operations we can use the diffPathArr
        val finalDirsList = if (isLoadOrCompaction) {
          val segmentFilePaths = diffPathArr.collect {
            case a if a.contains(".segment") =>
              SegmentFileStore.readSegmentFile(a.split(delimiter)(0))
                .getLocationMap.keySet().asScala.map(path => tablePath + path) ++
              Seq(a.split(delimiter)(0))
            case others => Seq(others.split(delimiter)(0))
          }.flatten
          takeRecurTraverseSnapshot(sqlContext,
            segmentFilePaths.toList,
            delimiter,
            recursive = true).toSet
        } else {
          diffPathArr
        }
        LOGGER.info(s"We have chmod ${finalDirsList.size} path(s) to current user")
        val user = currentUser.getShortUserName
        val groups = currentUser.getGroupNames
        val group = if (groups.isEmpty) {
          null
        } else {
          groups.head
        }
        var hdfs: FileSystem = null
        finalDirsList.foreach { pathStr =>
          val path = new Path(pathStr.split(delimiter)(0))
        try {
          hdfs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          if (!path.getName.contains(LockUsage.LOCK) && hdfs.exists(path)) {
            if (hdfs.getFileStatus(path).getOwner != user) {
              hdfs.setOwner(path, user, group)
            }
            setPermissions(hdfs, path)
            setACLGroupRights(currentUser, hdfs, path)
          }
        } catch {
          case e: FileNotFoundException =>
            LOGGER.warn("The file might be cleaned by other process," +
                        s" skipping applying acl for file ${ path.toString }")
        }
      }
    }
  }

  /**
   * Recur traverse the given path and get all paths which owner is what we hope.
   */
  private def recurTraverse(
      fs: FileSystem,
      path: Path,
      pathArr: ArrayBuffer[String],
      owner: String,
      pathToFilter: Option[String] = None,
      delimiter: String = "#~#", recursive: Boolean = false): Unit = {
    if (fs.isInstanceOf[DistributedFileSystem] || fs.isInstanceOf[ViewFileSystem]) {
      val pathFilter = new PathFilter() {
        override def accept(path: Path): Boolean = {
          return true
        }
      }
      val fileStatuses: Seq[FileStatus] = fs match {
        case dfs: DistributedFileSystem =>
          // Note: globLocatedStatus is not working for hadoop version 2.8.3 & LocatedFileStatus
          // is a heavy object which casues OOM if lakhs of files in the table
          dfs.globStatus(path, pathFilter)
        case vfs: ViewFileSystem =>
          vfs.globStatus(path, pathFilter)
      }
      // if the path does not exist then vfs.globStatus(path, pathFilter) will return null.
      if (null != fileStatuses) {
        fileStatuses.foreach { fileStatus =>
          if (null != fileStatus) {
            addFilePathToPathList(path,
              pathArr,
              owner,
              delimiter,
              fileStatus,
              recursive,
              fs,
              pathToFilter)
          }
        }
      }
    } else {
      // complete code in else block has been taken from org.apache.spark.util.Utils.recurTraverse
      if (!fs.exists(path)) {
        return
      }
      val fileStatus = fs.getFileStatus(path)
      addFilePathToPathList(path, pathArr, owner, delimiter, fileStatus)
      if (fs.isDirectory(path)) {
        // For InsertIntoHiveTable, will write data to .hive-staging.../-ext-.../... first, then run
        // load to move or replace the data, so we need to skip this temporary path.
        // For InsertIntoHadoopFsRelation, in current version, it will write data to the target path
        // directly.
        pathToFilter match {
          case Some(v) =>
            if (!path.getName.startsWith(v)) {
              fs.listStatus(path).foreach { fileStatus =>
                recurTraverse(
                  fs,
                  fileStatus.getPath,
                  pathArr,
                  owner,
                  pathToFilter,
                  delimiter
                )
              }
            }
          case None =>
            fs.listStatus(path).foreach { fileStatus =>
              recurTraverse(
                fs,
                fileStatus.getPath,
                pathArr,
                owner,
                pathToFilter,
                delimiter
              )
            }
        }
      }
    }
  }

  /**
   * this method will add a file/folder path in the path array
   *
   * @param path
   * @param pathArr
   * @param owner
   * @param delimiter
   * @param fileStatus
   * @return
   */
   private def addFilePathToPathList(path: Path,
      pathArr: ArrayBuffer[String],
      owner: String,
      delimiter: String,
      fileStatus: FileStatus,
      recursive: Boolean = false,
      fs: FileSystem = null,
      pathToFilter: Option[String] = None): Any = {
//    if (fileStatus.getOwner == owner) {
      if (fileStatus.isFile) {
        pathArr += (fileStatus.getPath.toString + delimiter + fileStatus.getModificationTime)
      } else {
        pathArr += (fileStatus.getPath.toString + delimiter + 1234567890L)
        if (recursive) {
          recurTraverse(
            fs,
            new Path(fileStatus.getPath.toString + "/*"),
            pathArr,
            owner,
            pathToFilter,
            delimiter,
            recursive
          )
        }
      }
//    }
  }

  /**
   * This method will form the path for all depths of a table whose permissions needs to
   * be taken care
   *
   * @param tablePath
   * @return
   */
  def getTablePathListForSnapshot(tablePath: String,
      partitionInfo: PartitionInfo, isLoadOrCompaction: Boolean = false): List[String] = {
    // e.g 1. dbName/tableName/Fact/Part0/Segment_0/carbondata_files
    // e.g 2. dbName/tableName/Metadata/{
    //                dictionary_files, tableStatus, schema, segments/partition_segment_files }
    // e.g 3. dbName/tableName/partCol1=val1/partCol2=val2/partCol3=val3/partCol4=val4 ....

    val depth: Integer = if (partitionInfo != null &&
                             partitionInfo.getColumnSchemaList.size() > 4) {
      partitionInfo.getColumnSchemaList.size()
    } else {
      if (isLoadOrCompaction) { 2 } else { 4 }
    }

    var path = tablePath
    List.tabulate(depth) {
      p =>
        path = path + "/*"
        path
    }
  }

  /**
   * This method will form the path for all depths of a bad records folder
   *
   * @param badRecordTablePath
   * @param carbonTableIdentifier
   * @return
   */
  def getBadRecordsPathListForSnapshot(badRecordTablePath: String,
      carbonTableIdentifier: CarbonTableIdentifier): List[String] = {
    // e.g badRecords_base_folder/dbName/tableName/segmentId/taskId/bad_record_file
    // till tableName permissions have already been given for logged in user
    val badRecordSegmentIdPath = badRecordTablePath + "/*"
    val badRecordTaskNoPath = badRecordSegmentIdPath + "/*"
    val badRecordFilesPath = badRecordTaskNoPath + "/*"
    List(badRecordSegmentIdPath,
      badRecordTaskNoPath,
      badRecordFilesPath)
  }

  def setPermissions(hdfs: FileSystem, path: Path): Unit = {
    if (isSecureModeEnabled()) {
      if (hdfs.isDirectory(path)) {
        setPermissions(hdfs, path,
          new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE))
      } else {
        setPermissions(hdfs, path,
          new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ))
      }
    } else {
      if (hdfs.isDirectory(path)) {
        setPermissions(hdfs, path,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
      } else {
        setPermissions(hdfs, path,
          new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ))
      }
    }
  }

  private def setPermissions(hdfs: FileSystem, path: Path, perm: FsPermission): Unit = {
    hdfs.setPermission(path, perm)
  }

  def setACLGroupRights(currentUser: UserGroupInformation, hdfs: FileSystem, path: Path): Unit = {
    if (isSecureModeEnabled && isACLSupported(path.toString)) {
      val carbonDataLoadGroup = CarbonProperties.getInstance.
        getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
          InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT)
      val permission = if (hdfs.isDirectory(path)) {
        "rwx"
      } else {
        "rw-"
      }
      currentUser.getGroupNames.foreach { groupName =>
        if (groupName != currentUser.getPrimaryGroupName &&
            groupName.startsWith(carbonDataLoadGroup)) {
          val aclList = new java.util.ArrayList[AclEntry];
          aclList.add(buildAclEntry(groupName, permission, AclEntryType.GROUP))
          hdfs.modifyAclEntries(path, aclList)
        }
      }
    }
  }

  private def buildAclEntry(
      group: String,
      permission: String,
      aclEntryType: AclEntryType): AclEntry = {
    new AclEntry.Builder().
      setType(AclEntryType.GROUP).
      setName(group).
      setPermission(FsAction.getFsAction(permission)).
      build();
  }

  def isSecureModeEnabled(): Boolean = {
    CarbonUserGroupInformation.getInstance.getLoginUser
      .getAuthenticationMethod.equals(AuthenticationMethod.KERBEROS)
  }

  def getPermissionsOnDatabase(): FsPermission = {
    if (isSecureModeEnabled()) {
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE)
    } else {
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
    }
  }

  def getPermissionsOnTable(): FsPermission = {
    if (isSecureModeEnabled()) {
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE)
    } else {
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
    }
  }

  /**
   * checks whether current user contains dataload group or not
   *
   * @param sparkContext
   * @return
   */
  def isCarbonDataLoadGroupExist(sparkContext: SparkContext): Boolean = {
    if (isSecureModeEnabled()) {
      val carbonDataLoadGroup = CarbonProperties.getInstance.
        getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
          InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT
        )
      // get user groups from server (any machine from cluster is ok)
      var userGroups: Array[String] = getUserGroups(sparkContext)

      userGroups.exists { groupName =>
        groupName.toLowerCase.startsWith(carbonDataLoadGroup)
      }
    } else {
      true
    }
  }

  def getUserGroups(sparkContext: SparkContext): Array[String] = {
    var userGroups: Array[String] = null
    // if spark-submit cluster mode no need to get the groups using NameNodeProxy via
    // GetUserMappingsProtocol as driver already in AM with proper ugi
    if (!"cluster".equals(sparkContext.getConf.get("spark.submit.deployMode"))) {
      val conf: Configuration = sparkContext.hadoopConfiguration
      var filesystem = FileSystem.get(conf)
      // In case of viewfs we have to select child to point to actual name node
      if (filesystem.isInstanceOf[ViewFileSystem]) {
        val childfs = filesystem.getChildFileSystems
        filesystem = childfs(0)
      }
      try {
        // if filesytem is hdfs then get groups from name node
        if (filesystem.isInstanceOf[DistributedFileSystem]) {
          userGroups = UserGroupUtils.getGroupsForUserFromNameNode(conf, filesystem.getUri)
        }
      } catch {
        case ex: Exception =>
          LOGGER
            .warn("No groups available for user " +
                  CarbonUserGroupInformation.getInstance.getCurrentUser.getShortUserName
            )
      }
    }
    if (null == userGroups) {
      userGroups = CarbonUserGroupInformation.getInstance.getCurrentUser.getGroupNames
    }
    userGroups
  }

  def createDirSetPermissionAndAddCarbonGroup(path: String, hdfs: FileSystem): Unit = {
    val fileType = FileFactory.getFileType(path)
    if (!FileFactory.isFileExist(path, fileType)) {
      FileFactory.createDirectoryAndSetPermission(path, getPermissionsOnDatabase)
      setACLGroupRights(CarbonUserGroupInformation.getInstance.getCurrentUser, hdfs, new Path(path))
    }
  }

  /**
   * the method creates
   *
   * @param sqlContext
   * @param carbonTableIdentifier
   * @param carbonBadRecordTablePath
   * @return
   */
  def createBadRecordsTablePath(sqlContext: SQLContext,
      carbonTableIdentifier: CarbonTableIdentifier,
      carbonBadRecordTablePath: String): String = {

    val splitpath = carbonBadRecordTablePath.split(CarbonCommonConstants.FILE_SEPARATOR)
    val tableName: String = splitpath(splitpath.length - 1)
    val dbName = splitpath(splitpath.length - 2)
    val basePath = carbonBadRecordTablePath
      .split(dbName + CarbonCommonConstants.FILE_SEPARATOR + tableName)(0)
    val carbonBadRecordDatabasePath = basePath + CarbonCommonConstants.FILE_SEPARATOR + dbName
    if (!FileFactory
      .isFileExist(carbonBadRecordDatabasePath,
        FileFactory.getFileType(carbonBadRecordDatabasePath)
      )) {
      CarbonUserGroupInformation.getInstance.getCurrentUser
        .doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            FileFactory.createDirectoryAndSetPermission(carbonBadRecordDatabasePath,
              getPermissionsOnDatabase())
          }
        });
    }

    if (!FileFactory
      .isFileExist(carbonBadRecordTablePath,
        FileFactory.getFileType(carbonBadRecordTablePath)
      )) {
      CarbonUserGroupInformation.getInstance.getCurrentUser
        .doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            FileFactory.createDirectoryAndSetPermission(carbonBadRecordTablePath,
              getPermissionsOnTable())
          }
        });
      val path = new Path(carbonBadRecordTablePath)
      setACLGroupRights(CarbonUserGroupInformation.getInstance.getLoginUser,
        path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration), path)
    }
    carbonBadRecordTablePath
  }

  def takeSnapshotBeforeOperation(operationContext: OperationContext,
      sparkSession: SparkSession,
      carbonTablePath: String,
      partitionInfo: PartitionInfo,
      carbonTableIdentifier: CarbonTableIdentifier, isLoadOrCompaction : Boolean = false): Unit = {
    val start = System.currentTimeMillis()
    val folderListbeforeCreate: List[String] = ACLFileUtils
      .getTablePathListForSnapshot(carbonTablePath, partitionInfo, isLoadOrCompaction)
    val pathArrBeforeCreateOperation = ACLFileUtils
      .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListbeforeCreate)
    operationContext.setProperty(getFolderListKey(carbonTableIdentifier), folderListbeforeCreate)
    operationContext
      .setProperty(getPathListKey(carbonTableIdentifier), pathArrBeforeCreateOperation)
    LOGGER
      .info("----------- Before Applying ACL : " + (System.currentTimeMillis() - start) +
            " for table :" + carbonTableIdentifier.getTableName)
  }

  def takeSnapAfterOperationAndApplyACL(sparkSession: SparkSession,
      operationContext: OperationContext,
      carbonTableIdentifier: CarbonTableIdentifier,
      recursive: Boolean = false,
      isLoadOrCompaction: Boolean = false,
      carbonTable: Option[CarbonTable] = None): Unit = {
    val start = System.currentTimeMillis()
    val folderPathsBeforeCreate = operationContext
      .getProperty(getFolderListKey(carbonTableIdentifier))
      .asInstanceOf[List[String]]
    val pathArrBeforeCreate = operationContext.getProperty(getPathListKey(carbonTableIdentifier))
      .asInstanceOf[ArrayBuffer[String]]
    val pathArrAfterCreate = ACLFileUtils
      .takeRecurTraverseSnapshot(sparkSession.sqlContext,
        folderPathsBeforeCreate,
        recursive = recursive)
    val tablePath = carbonTable match {
      case Some(table) => table.getTablePath
      case None => ""
    }
    changeOwnerRecursivelyAfterOperation(isLoadOrCompaction, sparkSession.sqlContext,
      pathArrBeforeCreate, pathArrAfterCreate, tablePath)
    if (carbonTableIdentifier != null) {
      LOGGER
        .info(
          "Time taken After Operation for Applying ACL : " + (System.currentTimeMillis() - start) +
          " for table :" + carbonTableIdentifier.getTableName)
    } else {
      LOGGER
        .info("Time taken After Operation for Applying ACL for MV : " +
              (System.currentTimeMillis() - start))
    }
  }

  def isACLSupported(tablePath: String): Boolean = {
    !StringUtils.isEmpty(tablePath) && !tablePath.startsWith("s3")
  }
}
