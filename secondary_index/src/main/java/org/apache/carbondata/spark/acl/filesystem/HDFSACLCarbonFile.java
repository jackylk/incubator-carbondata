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

package org.apache.carbondata.spark.acl.filesystem;

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.HDFSCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.spark.acl.ACLFileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HDFSACLCarbonFile extends HDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(HDFSACLCarbonFile.class.getName());

  public HDFSACLCarbonFile(String filePath) {
    super(filePath);
  }

  public HDFSACLCarbonFile(Path path) {
    super(path);
  }

  public HDFSACLCarbonFile(String filePath, Configuration hadoopConf) {
    super(filePath, hadoopConf);
  }

  public HDFSACLCarbonFile(final FileStatus hdfsFileStatus) {
    super(hdfsFileStatus);
  }

  @Override public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    CarbonFile carbonFile = null;
    if (null != parent) {
      carbonFile = new HDFSACLCarbonFile(parent);
    }
    return carbonFile;
  }

  @Override public boolean renameForce(final String changetoName) {
    return ACLFileUtils.renameForce(fileStatus, changetoName);
  }

  @Override public boolean createNewFile() {
    return ACLFileUtils.createNewFile(fileStatus, fs);
  }

  public boolean renameTo(final String changetoName) {
    return ACLFileUtils.renameTo(fileStatus, changetoName);
  }

  public boolean delete() {
    return ACLFileUtils.delete(fileStatus);
  }

  @Override public boolean setLastModifiedTime(final long timestamp) {
    return ACLFileUtils.setLastModifiedTime(fileStatus, fs, timestamp);
  }

  @Override public DataOutputStream getDataOutputStream(final String path,
      final FileFactory.FileType fileType, final int bufferSize, final boolean append)
      throws IOException {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<DataOutputStream>() {
        @Override public DataOutputStream run() throws Exception {
          return HDFSACLCarbonFile.super.getDataOutputStream(path, fileType, bufferSize, append);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return null;
    }
  }

  @Override public DataOutputStream getDataOutputStream(final String path,
      final FileFactory.FileType fileType) throws IOException {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<DataOutputStream>() {
        @Override public DataOutputStream run() throws Exception {
          return HDFSACLCarbonFile.super.getDataOutputStream(path, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return null;

    }
  }

  @Override public DataOutputStream getDataOutputStream(final String path,
      final FileFactory.FileType fileType, final int bufferSize, final long blockSize)
      throws IOException {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<DataOutputStream>() {
        @Override public DataOutputStream run() throws Exception {
          return HDFSACLCarbonFile.super.getDataOutputStream(path, fileType, bufferSize, blockSize);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return null;

    }
  }

  @Override public boolean deleteFile(String filePath, final FileFactory.FileType fileType)
      throws IOException {
    final String localFilePath = filePath.replace("\\", "/");
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return HDFSACLCarbonFile.super.deleteFile(localFilePath, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    }
  }

  @Override public boolean mkdirs(String filePath, final FileFactory.FileType fileType)
      throws IOException {
    final String localFilePath = filePath.replace("\\", "/");
    final boolean doAs = true;
    try {
      PrivilegedExceptionAction<Boolean> privObject = new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return HDFSACLCarbonFile.super.mkdirs(localFilePath, fileType);
        }
      };
      if (doAs) {
        return PrivilegedFileOperation.execute(privObject);
      } else {
        return privObject.run();
      }
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      throw new IOException(e.getMessage());
    }
  }

  @Override public DataOutputStream getDataOutputStreamUsingAppend(String path,
      final FileFactory.FileType fileType) throws IOException {
    final String localFilePath = path.replace("\\", "/");
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<DataOutputStream>() {
        @Override public DataOutputStream run() throws Exception {
          return HDFSACLCarbonFile.super.getDataOutputStreamUsingAppend(localFilePath, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return null;
    }
  }

  @Override public boolean createNewLockFile(String filePath, final FileFactory.FileType fileType)
      throws IOException {
    final String localFilePath = filePath.replace("\\", "/");
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return HDFSACLCarbonFile.super.createNewLockFile(localFilePath, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    }
  }

  @Override public boolean createNewFile(final String filePath, final FileFactory.FileType fileType,
      final boolean doAs, final FsPermission permission) throws IOException {
    try {
      PrivilegedExceptionAction<Boolean> privObject = new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return HDFSACLCarbonFile.super.createNewFile(filePath, fileType, doAs, permission);
        }
      };
      if (doAs) {
        return PrivilegedFileOperation.execute(privObject);
      } else {
        return privObject.run();
      }
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      throw new IOException(e.getMessage());
    }
  }

}
