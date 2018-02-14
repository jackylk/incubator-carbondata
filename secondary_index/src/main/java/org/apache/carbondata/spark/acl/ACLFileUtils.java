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

package org.apache.carbondata.spark.acl;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.spark.acl.filesystem.PrivilegedFileOperation;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * ACLFileUtils: to do PrivilegedFileOperation on the file
 */
public class ACLFileUtils {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ACLFileUtils.class.getName());

  /**
   * The method creates new file
   *
   * @param fileStatus FileStatus object of current file
   * @param fs         FileSystem object of current file
   * @return <true> if file is file created successfully <false> if file already exist or file
   * creation failed due to Exception happened during file creation.
   */
  public static boolean createNewFile(final FileStatus fileStatus, final FileSystem fs) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          Path path = fileStatus.getPath();
          return fs.createNewFile(path);
        }
      });
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred: " + e.getMessage());
      return false;
    }
  }

  /**
   * Rename current fileName to the given name.
   *
   * @param fileStatus   FileStatus object of current file
   * @param changeToName changed file name
   * @return <true> if rename is success <false> if rename fails due to Exception happened
   * during file rename.
   */
  public static boolean renameTo(final FileStatus fileStatus, final String changeToName) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          return fs.rename(fileStatus.getPath(), new Path(changeToName));
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred: " + e.getMessage());
      return false;
    }
  }

  /**
   * The method deletes the given file
   *
   * @param fileStatus FileStatus object of current file
   * @return <true> if file deletion is success <false> if deletion fails due to Exception happened
   * during file deletion.
   */
  public static boolean delete(final FileStatus fileStatus) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          return fs.delete(fileStatus.getPath(), true);
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred: " + e.getMessage());
      return false;
    }
  }

  /**
   * The method set the modified time in file
   *
   * @param fileStatus FileStatus object of current file
   * @param fs         FileSystem object of current file
   * @return <true> if file modified time is set successfully <false> if file modified time set
   * failed due to Exception happened during file creation.
   */
  public static boolean setLastModifiedTime(final FileStatus fileStatus, final FileSystem fs,
      final long timestamp) {
    try {
      PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          fs.setTimes(fileStatus.getPath(), timestamp, timestamp);
          return null;
        }
      });
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred: " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Set specified FsPermission to the specified path
   *
   * @param directoryPath <String> directory path
   * @param permission    FsPermission object
   * @throws IOException throw IOException is failures happen during the setting file permission
   */
  public static void setPermission(String directoryPath, FsPermission permission)
      throws IOException {
    try {
      Path path = new Path(directoryPath);
      FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
      if (fs.exists(path)) {
        // fs.setOwner(path, username, group);
        fs.setPermission(path, permission);
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred : " + e.getMessage());
      throw e;
    }
  }
}

