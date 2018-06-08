package org.apache.carbondata.service.obs;

import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ObsTest {

  public static void main(String[] args) throws IOException {

    String filePath = "s3a://carbon-select/conf/client/carbonselect.properties";
    String targetFolder = "/home/david/Documents/code/carbonstore/select/service/target";

    Configuration hadoopConf = FileFactory.getConfiguration();
    hadoopConf.addResource(new Path("/data1/carbonselect/conf/server/core-default.xml"));
    Path path = new Path(filePath);
    FileSystem fileSystem = path.getFileSystem(hadoopConf);

    fileSystem.copyToLocalFile(false, path, new Path(targetFolder), false);
  }
}
