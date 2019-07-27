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

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.OBSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.impl.DefaultFileTypeProvider;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.spark.acl.filesystem.AlluxioACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.HDFSACLCarbonFile;
import org.apache.carbondata.spark.acl.filesystem.ViewFSACLCarbonFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ACLFileFactory extends DefaultFileTypeProvider {

  private static final Log LOG = LogFactory.getLog(FileFactory.class);

  public CarbonFile getCarbonFile(String path, Configuration conf) {
    // Handle the custom file type first
    if (isPathSupported(path)) {
      return customFileTypeProvider.getCarbonFile(path, conf);
    }

    FileFactory.FileType fileType = FileFactory.getFileType(path);
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case S3:
        return new S3CarbonFile(path, conf);
      case OBS:
        return new OBSCarbonFile(path, conf);
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
