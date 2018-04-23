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
