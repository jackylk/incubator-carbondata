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

package org.apache.spark.sql.acl

import java.security.PrivilegedExceptionAction

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryType, FsAction, FsPermission}
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.spark.sql.{SparkSession, SQLContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableAddColumnPostEvent, OperationContext}
import org.apache.carbondata.spark.acl.{CarbonBadRecordStorePath, CarbonUserGroupInformation, InternalCarbonConstant, UserGroupUtils}


object ACLFileUtils {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val folderListBeforeOperation = "folderListBeforeOperation"
  val pathArrBeforeOperation = "pathArrBeforeOperation"
  def proxyOperate(
      loginUser: UserGroupInformation,
      currentUser: UserGroupInformation,
      msg: String,
      doOriLogic: Boolean = true)(f: => Unit): Unit = {
    Utils.proxyOperate(loginUser, currentUser, msg, doOriLogic)(f)
  }

  def takeRecurTraverseSnapshot(sqlContext: SQLContext,
      folderPaths: List[Path], delimiter: String = "#~#"): ArrayBuffer[String] = {
    val loginUser = CarbonUserGroupInformation.getInstance.getLoginUser
    val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
    val oriPathArr = new ArrayBuffer[String]()
    Utils.proxyOperate(loginUser, currentUser,
      s"Use login user ${loginUser.getShortUserName} as a proxy user as we need " +
      s"permission to operate the given path", true) {
      var hdfs: FileSystem = null
      folderPaths.foreach { path =>
        hdfs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        recurTraverse(
          hdfs,
          path,
          oriPathArr,
          loginUser.getShortUserName,
          None,
          delimiter
        )
      }
    }
    oriPathArr
  }


  def changeOwnerRecursivelyAfterOperation(sqlContext: SQLContext,
      oriPathArr: ArrayBuffer[String], curPathArr: ArrayBuffer[String], delimiter: String = "#~#") {
    val loginUser = CarbonUserGroupInformation.getInstance.getLoginUser
    val currentUser = CarbonUserGroupInformation.getInstance.getCurrentUser
    Utils.proxyOperate(loginUser, currentUser,
      s"Use login user ${loginUser.getShortUserName} as a proxy user as we need " +
      s"permission to operate the given path", true) {
      val diffPathArr = curPathArr.toSeq.diff(oriPathArr.toSeq)
      LOGGER.info(s"We have chmod ${diffPathArr.size} path(s) to current user")
      val user = currentUser.getShortUserName
      val groups = currentUser.getGroupNames
      val group = if (groups.isEmpty) {
        null
      } else {
        groups.head
      }
      var hdfs: FileSystem = null
      diffPathArr.foreach { pathStr =>
        val path = new Path(pathStr.split(delimiter)(0))
        hdfs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        if (hdfs.exists(path)) {
          if (hdfs.getFileStatus(path).getOwner != user) {
            hdfs.setOwner(path, user, group)
          }
          setPermissions(hdfs, path)
          setACLGroupRights(currentUser, hdfs, path)
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
      delimiter: String = "#~#"): Unit = {
    if (fs.isInstanceOf[DistributedFileSystem] || fs.isInstanceOf[ViewFileSystem]) {
      val pathFilter = new PathFilter() {
        override def accept(path: Path): Boolean = {
          return true
        }
      }
      val fileStatuses: Seq[FileStatus] = fs match {
        case dfs: DistributedFileSystem =>
          dfs.globLocatedStatus(path, pathFilter, false)
        case vfs: ViewFileSystem =>
          vfs.globStatus(path, pathFilter)
      }
      // if the path does not exist then vfs.globStatus(path, pathFilter) will return null.
      if (null != fileStatuses) {
        fileStatuses.foreach { fileStatus =>
          if (null != fileStatus) {
            addFilePathToPathList(path, pathArr, owner, delimiter, fileStatus)
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
      fileStatus: FileStatus): Any = {
//    if (fileStatus.getOwner == owner) {
      if (fileStatus.isFile) {
        pathArr += (fileStatus.getPath.toString + delimiter + fileStatus.getModificationTime)
      } else {
        pathArr += (fileStatus.getPath.toString + delimiter + 1234567890L)
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
  def getTablePathListForSnapshot(tablePath: CarbonTablePath): List[Path] = {
    // e.g 1. dbName/tableName/Fact/Part0/Segment_0/carbondata_files
    // e.g 2. dbName/tableName/Metadata/{dictionary_files, tableStatus, schema}
    val carbonTablePath = new Path(tablePath.getPath)
    val factAndMetadataDir = new Path(carbonTablePath.toString + "/*")
    val metadataContentAndPartitionDir = new Path(factAndMetadataDir.toString + "/*")
    val segmentDirPath = new Path(metadataContentAndPartitionDir.toString + "/*")
    val carbonDataFilePath = new Path(segmentDirPath.toString + "/*")
    List(carbonTablePath,
      factAndMetadataDir,
      metadataContentAndPartitionDir,
      segmentDirPath,
      carbonDataFilePath)
  }

  /**
   * This method will form the path for all depths of a bad records folder
   *
   * @param badRecordTablePath
   * @param carbonTableIdentifier
   * @return
   */
  def getBadRecordsPathListForSnapshot(badRecordTablePath: String,
      carbonTableIdentifier: CarbonTableIdentifier): List[Path] = {
    // e.g badRecords_base_folder/dbName/tableName/segmentId/taskId/bad_record_file
    // till tableName permissions have already been given for logged in user
    val badRecordSegmentIdPath = new Path(badRecordTablePath.toString + "/*")
    val badRecordTaskNoPath = new Path(badRecordSegmentIdPath.toString + "/*")
    val badRecordFilesPath = new Path(badRecordTaskNoPath.toString + "/*")
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
    if (isSecureModeEnabled) {
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
   * @param conf
   * @return
   */
  def isCarbonDataLoadGroupExist(conf: Configuration): Boolean = {
    if (isSecureModeEnabled()) {
      val carbonDataLoadGroup = CarbonProperties.getInstance.
        getProperty(InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME,
          InternalCarbonConstant.CARBON_DATALOAD_GROUP_NAME_DEFAULT
        )
      // get user groups from server (any machine from cluster is ok)
      var filesystem = FileSystem.get(conf)
      // In case of viewfs we have to select child to point to actual name node
      if (filesystem.isInstanceOf[ViewFileSystem]) {
        val childfs = filesystem.getChildFileSystems
        filesystem = childfs(0)
      }
      var userGroups: Array[String] = null
      try {
        // if filesytem is hdfs then get groups from name node
        if (filesystem.isInstanceOf[DistributedFileSystem]) {
          userGroups = UserGroupUtils.getGroupsForUserFromNameNode(conf, filesystem.getUri)
        }
      } catch {
        case ex: Exception =>
          LOGGER
            .warn("No groups available for user " +
                  CarbonUserGroupInformation.getInstance.getCurrentUser.getShortUserName)
      }
      if (null == userGroups) {
        userGroups = CarbonUserGroupInformation.getInstance.getCurrentUser.getGroupNames
      }
      userGroups.exists { groupName =>
        groupName.toLowerCase.startsWith(carbonDataLoadGroup)
      }
    } else {
      true
    }
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
   * @param carbonBadRecordPath
   * @return
   */
  def createBadRecordsTablePath(sqlContext: SQLContext,
      carbonTableIdentifier: CarbonTableIdentifier,
      carbonBadRecordPath: String): String = {

    val carbonBadRecordStorePath = new CarbonBadRecordStorePath(carbonBadRecordPath);
    val carbonBadRecordDatabasePath = carbonBadRecordStorePath
      .getCarbonBadRecordStoreDatabasePath(carbonTableIdentifier);

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
    val carbonBadRecordTablePath = carbonBadRecordStorePath
      .getCarbonBadRecordStoreTablePath(carbonTableIdentifier)
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

  def takeSnapshotBeforeOpeartion(operationContext: OperationContext,
      sparkSession: SparkSession,
      carbonTablePath: CarbonTablePath): Unit = {

    val folderListbeforeCreate: List[Path] = ACLFileUtils
      .getTablePathListForSnapshot(carbonTablePath)
    val pathArrBeforeCreateOperation = ACLFileUtils
      .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListbeforeCreate)
    operationContext.setProperty(folderListBeforeOperation, folderListbeforeCreate)
    operationContext.setProperty(pathArrBeforeOperation, pathArrBeforeCreateOperation)
  }

  def takeSnapAfterOperationAndApplyACL(sparkSession: SparkSession,
      operationContext: OperationContext): Unit = {
    val folderPathsBeforeCreate = operationContext.getProperty(folderListBeforeOperation)
      .asInstanceOf[List[Path]]
    val pathArrBeforeCreate = operationContext.getProperty(pathArrBeforeOperation)
      .asInstanceOf[ArrayBuffer[String]]
    val pathArrAfterCreate = ACLFileUtils
      .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderPathsBeforeCreate)

    ACLFileUtils.changeOwnerRecursivelyAfterOperation(sparkSession.sqlContext,
      pathArrBeforeCreate, pathArrAfterCreate)
  }
}
