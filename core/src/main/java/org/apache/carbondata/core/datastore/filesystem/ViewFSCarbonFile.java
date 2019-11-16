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
package org.apache.carbondata.core.datastore.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.log4j.Logger;

public class ViewFSCarbonFile extends AbstractDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ViewFSCarbonFile.class.getName());

  public ViewFSCarbonFile(String filePath) {
    super(filePath);
  }

  public ViewFSCarbonFile(Path path) {
    super(path);
  }

  public ViewFSCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  /**
   * @param listStatus
   * @return
   */
  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new ViewFSCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    CarbonFile[] files = listFiles();
    if (files != null && files.length >= 1) {
      List<CarbonFile> fileList = new ArrayList<CarbonFile>(files.length);
      for (int i = 0; i < files.length; i++) {
        if (fileFilter.accept(files[i])) {
          fileList.add(files[i]);
        }
      }
      if (fileList.size() >= 1) {
        return fileList.toArray(new CarbonFile[fileList.size()]);
      } else {
        return new CarbonFile[0];
      }
    }
    return files;
  }

  @Override public CarbonFile getParentFile() {
    if (fileStatus == null) {
      throw new RuntimeException("Unable to get file status, Please check logs for details.");
    }
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new ViewFSCarbonFile(parent);
  }

  @Override
  public boolean renameForce(String changeToName) {
    FileSystem fs;
    if (fileStatus == null) {
      throw new RuntimeException("Unable to get file status, Please check logs for details.");
    }
    try {
      fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
      if (changeToName.startsWith(CarbonCommonConstants.LUXORFS_PREFIX) ||
          fs instanceof ViewFileSystem) {
        fs.delete(new Path(changeToName), true);
        fs.rename(fileStatus.getPath(), new Path(changeToName));
        return true;
      } else {
        LOGGER.warn("Unrecognized file system for path: " + filePath);
        return false;
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured" + e.getMessage(), e);
      return false;
    }
  }
}
