package org.apache.hadoop.hbase.coprocessor;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class CarbonPropertiesLoader {
  private static Logger LOGGER =
      LogServiceFactory.getLogService(CarbonPropertiesLoader.class.getCanonicalName());

  /**
   * load carbon config from an obs folder.
   * If it has some exception, it will ignore the exception and only write the error log.
   * It will not stop the start up flow of leader
   */
  public static void loadConfig(final String endPoint, final String ak, final String sk,
      String configFolder) {
    // prepare a hadoop configuration
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set(SparkS3Constants.FS_OBS_END_POINT, endPoint);
    hadoopConf.set(SparkS3Constants.FS_OBS_AK, ak);
    hadoopConf.set(SparkS3Constants.FS_OBS_AK, sk);
    hadoopConf.set("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    // load config for carbon
    loadCarbonConfig(configFolder, hadoopConf);
    LOGGER.info("Finished to load config files from folder " + configFolder);
  }

  /**
   * load carbon.properties in obs into CarbonProperties
   */
  private static void loadCarbonConfig(final String configFolder, final Configuration hadoopConf) {
    String configFile = configFolder + CarbonCommonConstants.FILE_SEPARATOR + "carbon.properties";
    Properties properties = loadProperties(configFile, hadoopConf);
    // add all property into CarbonProperties
    Iterator<String> iterator = properties.stringPropertyNames().iterator();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    LOGGER.info("start to load config file " + configFile);
    while (iterator.hasNext()) {
      String key = iterator.next();
      String value = properties.getProperty(key).trim();
      carbonProperties.addProperty(key, value);
      LOGGER.info(key + "=" + value);
    }
  }

  /**
   * load a property file in obs into Properties object
   */
  private static Properties loadProperties(final String configFile,
      final Configuration hadoopConf) {
    Properties properties = new Properties();
    DataInputStream dataInputStream = null;
    try {
      CarbonFile file = FileFactory.getCarbonFile(configFile, hadoopConf);
      dataInputStream =
          file.getDataInputStream(configFile, FileFactory.getFileType(configFile), 4096,
              hadoopConf);
      properties.load(dataInputStream);
    } catch (IOException e) {
      LOGGER.error("Failed to load config file " + configFile, e);
    } finally {
      if (dataInputStream != null) {
        try {
          dataInputStream.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close config file " + configFile, e);
        }
      }
    }
    return properties;
  }
}
