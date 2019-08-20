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

package org.apache.carbondata.leo.queryserver.util;

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
import org.apache.spark.sql.SparkSession;

public class ConfigUtils {

  private static Logger LOGGER =
      LogServiceFactory.getLogService(ConfigUtils.class.getCanonicalName());

  /**
   * load fleet config from an obs folder.
   * If it has some exception, it will ignore the exception and only write the error log.
   * It will not stop the start up flow of leader
   */
  public static void loadConfig(final String endPoint, final String ak, final String sk,
      String configFolder, SparkSession.Builder builder) {
    // prepare a hadoop configuration
    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set("fs.obs.endpoint", endPoint);
    hadoopConf.set("fs.obs.access.key", ak);
    hadoopConf.set("fs.obs.secret.key", sk);
    hadoopConf.set("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    // load config for spark
    loadSparkConfig(configFolder, hadoopConf, builder);
    // load config for carbon
    loadCarbonConfig(configFolder, hadoopConf);
    LOGGER.info("Finished to load config files from folder " + configFolder);
  }

  /**
   * load spark-defaults.conf in obs to overwrite the default config of Spark
   */
  private static void loadSparkConfig(final String configFolder, final Configuration hadoopConf,
      final SparkSession.Builder builder) {
    String configFile = configFolder + CarbonCommonConstants.FILE_SEPARATOR + "spark-defaults.conf";
    Properties properties = loadProperties(configFile, hadoopConf);
    // add all property into SparkConf
    Iterator<String> iterator = properties.stringPropertyNames().iterator();
    LOGGER.info("start to load config file " + configFile);
    while (iterator.hasNext()) {
      String key = iterator.next();
      String value = properties.getProperty(key).trim();
      builder.config(key, value);
      LOGGER.info(key + "=" + value);
    }
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
