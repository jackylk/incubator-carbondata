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
package org.apache.carbondata.spark.acl.filesystem;

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.AlluxioCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.spark.acl.ACLFileUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class AlluxioACLCarbonFile extends AlluxioCarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AlluxioACLCarbonFile.class.getName());

  public AlluxioACLCarbonFile(String filePath) {
    super(filePath);
  }

  public AlluxioACLCarbonFile(Path path) {
    super(path);
  }

  public AlluxioACLCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  @Override public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new AlluxioACLCarbonFile(parent);
  }

  @Override public boolean renameForce(final String changetoName) {
    return ACLFileUtils.renameForce(fileStatus, changetoName);
  }

  @Override public boolean createNewFile() {
    return ACLFileUtils.createNewFile(fileStatus, fs);
  }

  public boolean renameTo(final String changedToName) {
    return ACLFileUtils.renameTo(fileStatus, changedToName);
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
          return AlluxioACLCarbonFile.super.getDataOutputStream(path, fileType, bufferSize, append);
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
          return AlluxioACLCarbonFile.super.getDataOutputStream(path, fileType);
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
          return AlluxioACLCarbonFile.super
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
          return AlluxioACLCarbonFile.super.deleteFile(localFilePath, fileType);
        }
      });
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured : " + e.getMessage());
      return false;
    }
  }

  @Override public boolean mkdirs(String filePath)
      throws IOException {
    final String localFilePath = filePath.replace("\\", "/");
    final boolean doAs = true;
    try {
      PrivilegedExceptionAction<Boolean> privObject = new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          return AlluxioACLCarbonFile.super.mkdirs(localFilePath);
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
          return AlluxioACLCarbonFile.super.getDataOutputStreamUsingAppend(localFilePath, fileType);
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
          return AlluxioACLCarbonFile.super.createNewLockFile(localFilePath, fileType);
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
          return AlluxioACLCarbonFile.super.createNewFile(filePath, fileType, doAs, permission);
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
