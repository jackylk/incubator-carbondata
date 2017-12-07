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

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileHolderImpl;
import org.apache.carbondata.core.datastore.impl.FileTypeInerface;
import org.apache.carbondata.spark.acl.filesystem.AlluxioACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.DFSACLFileHolderImpl;
import org.apache.carbondata.spark.acl.filesystem.HDFSACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.ViewFSACLCarbonFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class ACLFileFactory implements FileTypeInerface {

  private static final Log LOG = LogFactory.getLog(FileFactory.class);

  public  FileHolder getFileHolder(FileFactory.FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new FileHolderImpl();
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
        return new DFSACLFileHolderImpl();
      default:
        return new FileHolderImpl();
    }
  }

  public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case HDFS:
      case S3:
        return new HDFSACLCarbonFile(path);
      case ALLUXIO:
        return new AlluxioACLCarbonFile(path);
      case VIEWFS:
        return new ViewFSACLCarbonFile(path);
      default:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
    }
  }

  public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType, Configuration conf) {
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case HDFS:
      case S3:
        return new HDFSACLCarbonFile(path, conf);
      case ALLUXIO:
        return new AlluxioACLCarbonFile(path);
      case VIEWFS:
        return new ViewFSACLCarbonFile(path);
      default:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
    }
  }

}
