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
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

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

  /**
   * Rename forcefully the current fileName to the given name.
   *
   * @param fileStatus   FileStatus object of current file
   * @param changeToName changed file name
   * @return <true> if rename is success <false> if rename fails due to Exception happened
   * during file rename.
   */
  public static boolean renameForce(final FileStatus fileStatus, final String changeToName) {
    try {
      return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override public Boolean run() throws Exception {
          FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
          if (fs instanceof DistributedFileSystem) {
            ((DistributedFileSystem) fs).rename(fileStatus.getPath(), new Path(changeToName),
                org.apache.hadoop.fs.Options.Rename.OVERWRITE);
            return true;
          } else if (fs instanceof ViewFileSystem) {
            fs.delete(new Path(changeToName), true);
            fs.rename(fileStatus.getPath(), new Path(changeToName));
            return true;
          } else {
            return false;
          }
        }
      });
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }
}

