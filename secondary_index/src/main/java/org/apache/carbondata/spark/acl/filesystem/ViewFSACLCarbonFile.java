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
import org.apache.carbondata.core.datastore.filesystem.ViewFSCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;

public class ViewFSACLCarbonFile extends ViewFSCarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ViewFSACLCarbonFile.class.getName());

  public ViewFSACLCarbonFile(String filePath) {
    super(filePath);
  }

  public ViewFSACLCarbonFile(Path path) {
    super(path);
  }

  public ViewFSACLCarbonFile(final FileStatus fileStatus) {
    super(fileStatus);
  }


  @Override public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    CarbonFile carbonFile = null;
    if (null != parent) {
      carbonFile = new ViewFSACLCarbonFile(parent);
    }
    return carbonFile;
  }

  @Override public boolean renameForce(final String changetoName) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          if (fs instanceof ViewFileSystem) {
            fs.delete(new Path(changetoName), true);
            fs.rename(fileStatus.getPath(), new Path(changetoName));
            return true;
          } else {
            return false;
          }
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occured" + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  @Override public boolean createNewFile() {
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
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  public boolean renameTo(final String changetoName) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          return fs.rename(fileStatus.getPath(), new Path(changetoName));
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  public boolean delete() {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          return fs.delete(fileStatus.getPath(), true);
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  @Override public boolean setLastModifiedTime(final long timestamp) {
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
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
    return true;
  }

  @Override public DataOutputStream getDataOutputStream(final String path,
      final FileFactory.FileType fileType, final int bufferSize, final boolean append)
      throws IOException {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<DataOutputStream>() {
        @Override public DataOutputStream run() throws Exception {
          return ViewFSACLCarbonFile.super.getDataOutputStream(path, fileType, bufferSize, append);
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
          return ViewFSACLCarbonFile.super.getDataOutputStream(path, fileType);
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
          return ViewFSACLCarbonFile.super
              .getDataOutputStream(path, fileType, bufferSize, blockSize);
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
          return ViewFSACLCarbonFile.super.deleteFile(localFilePath, fileType);
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
          return ViewFSACLCarbonFile.super.mkdirs(localFilePath, fileType);
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
          return ViewFSACLCarbonFile.super.getDataOutputStreamUsingAppend(localFilePath, fileType);
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
          return ViewFSACLCarbonFile.super.createNewLockFile(localFilePath, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    }
  }

  @Override
  public void setPermission(String directoryPath, FsPermission permission, String username,
      String group) throws IOException {
    try {
      Path path = new Path(directoryPath);
      FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
      if (fs.exists(path)) {
        fs.setOwner(path, username, group);
        fs.setPermission(path, permission);
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred : " + e.getMessage());
      throw e;
    }
  }

  @Override public boolean createNewFile(final String filePath, final FileFactory.FileType fileType,
      final boolean doAs, final FsPermission permission) throws IOException {
    try {
      PrivilegedExceptionAction<Boolean> privObject = new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return ViewFSACLCarbonFile.super.createNewFile(filePath, fileType, doAs, permission);
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
