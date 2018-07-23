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

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.impl.DFSFileReaderImpl;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileReaderImpl;
import org.apache.carbondata.core.datastore.impl.FileTypeInterface;
import org.apache.carbondata.spark.acl.filesystem.AlluxioACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.HDFSACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.ViewFSACLCarbonFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class ACLFileFactory implements FileTypeInterface {

  private static final Log LOG = LogFactory.getLog(FileFactory.class);

  public FileReader getFileHolder(FileFactory.FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new FileReaderImpl();
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
        return new DFSFileReaderImpl();
      default:
        return new FileReaderImpl();
    }
  }

  public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case S3:
        return new S3CarbonFile(path);
      case HDFS:
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
      case S3:
        return new S3CarbonFile(path, conf);
      case HDFS:
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
