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

package org.apache.carbondata.leo.queryserver;

import java.io.IOException;
import java.net.InetAddress;

import com.huawei.cloud.obs.OBSUtil;
import org.apache.carbondata.leo.job.query.JobConf;

import com.huawei.cloud.obs.OBSSparkConstants;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.leo.queryserver.util.StoreConf;

import org.apache.carbondata.leo.queryserver.util.ConfigUtils;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.leo.LeoEnv;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.apache.carbondata.leo.job.query.JobConf.LEO_QUERY_RESULT_BUCKET_PREFIX;
import static org.apache.carbondata.leo.job.query.JobConf.LEO_QUERY_RESULT_BUCKET_PREFIX_DEFAULT;

public class Main {

  private static Logger LOGGER =
      LogServiceFactory.getLogService(Main.class.getCanonicalName());

  private static ConfigurableApplicationContext context;

  private static SparkSession session;

  public static void main(String[] args) {
    if (args.length != 7) {
      LOGGER.error("Usage: Main " +
          "<cluster.name> " +
          "<spark.hadoop.fs.s3a.endpoint> " +
          "<spark.hadoop.fs.s3a.access.key> " +
          "<spark.hadoop.fs.s3a.secret.key> " +
          "<spark.hadoop.fs.s3a.region> " +
          "<fleet.conf.folder> " +
          "<hbase.zookeeper.quorum> ");
      return;
    }

    try {
      String ip = InetAddress.getLocalHost().getHostAddress();
      LOGGER.info("Driver IP: " + ip);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }

    try {
      createSession(args);
      // Start Spring
      String storeConfFile = System.getProperty(StoreConf.STORE_CONF_FILE);
      start(SpringbootApplication.class, storeConfFile);
      //create obs bucket and dir to store result, if exist, it will not create.
      String clusterName = args[0];
      String bucket =
          session.conf().get(LEO_QUERY_RESULT_BUCKET_PREFIX, LEO_QUERY_RESULT_BUCKET_PREFIX_DEFAULT)
              + args[4];
      OBSUtil.createBucket(bucket, session, args[4]);
      System.setProperty(JobConf.LEO_QUERY_BUCKET_NAME, bucket);
      OBSUtil.createDir(bucket, JobConf.QUERY_RESULT_DIR_PREFIX + clusterName + "/", session);
      LOGGER.info("cluster started: " + clusterName);
      System.setProperty(JobConf.LEO_CLUSTER_NAME, clusterName);
      Thread.sleep(Long.MAX_VALUE);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  private static <T> void start(final Class<T> classTag, String storeConfFile) {
    if (storeConfFile != null) {
      System.setProperty("carbonstore.conf.file", storeConfFile);
    }
    Thread thread = new Thread() {
      public void run() {
        context = SpringApplication.run(classTag);
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

  public static void stop() {
    SpringApplication.exit(context);
  }

  private static void createSession(String[] args) throws Exception {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "");
    // default config
    SparkSession.Builder builder = SparkSession.builder()
        .appName("Leo-leader")
        .master("yarn")
        .config(OBSSparkConstants.END_POINT, args[1])
        .config(OBSSparkConstants.AK, args[2])
        .config(OBSSparkConstants.SK, args[3])
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
        .config(OBSSparkConstants.OBS_END_POINT, args[1])
        .config(OBSSparkConstants.OBS_AK, args[2])
        .config(OBSSparkConstants.OBS_SK, args[3])
        .config("spark.hadoop.fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
        .config("spark.ui.port", 9876)
        .config("spark.sql.crossJoin.enabled", "true")
        .config("hive.warehouse.subdir.inherit.perms", false)
        .config("carbon.source.ak", args[2])
        .config("carbon.source.sk", args[3])
        .config("hbase.zookeeper.quorum", args[6]);
    // load config from obs
    ConfigUtils.loadConfig(args[1], args[2], args[3], args[5], builder);
    // create the SparkSession basing the configuration
    session = LeoEnv.getOrCreateLeoSession(builder);
  }

  public static SparkSession getSession() {
    return session;
  }

}
