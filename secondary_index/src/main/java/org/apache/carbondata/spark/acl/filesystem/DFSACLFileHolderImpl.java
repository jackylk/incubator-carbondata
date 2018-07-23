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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.DFSFileReaderImpl;

import org.apache.hadoop.fs.FSDataInputStream;

public class DFSACLFileHolderImpl extends DFSFileReaderImpl {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DFSACLFileHolderImpl.class.getName());

  public DFSACLFileHolderImpl() {
    super();
  }

  public FSDataInputStream updateCache(final String filePath) {
    FSDataInputStream fileChannel = fileNameAndStreamCache.get(filePath);
    if (null == fileChannel) {
      try {
        return PrivilegedFileOperation.execute(new PrivilegedExceptionAction<FSDataInputStream>() {
              @Override public FSDataInputStream run() throws Exception {
                return getFromBase(filePath);
              }
            });
      } catch (InterruptedException e) {
        LOGGER.error("Exception occured : " + e.getMessage());
      } catch (IOException e) {
        LOGGER.error("Exception occured : " + e.getMessage());
      }
    }
    return fileChannel;
  }

  private FSDataInputStream getFromBase(final String filePath) throws IOException {
    return super.updateCache(filePath);
  }
}
