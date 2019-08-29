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
package com.huawei.cloud.obs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;

import com.huawei.cloud.RestConstants;
import com.huawei.cloud.credential.LoginRequestManager;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.fs.NewBucketRequest;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class OBSUtil {
  private static final Logger LOGGER = LogServiceFactory.getLogService(OBSUtil.class.getName());

  public static void createBucket(String bucketName, SparkSession session, String regionName)
      throws IOException {
    try (ObsClient obsClient = createObsClient(session)) {
      if (!obsClient.headBucket(bucketName)) {
        if (Boolean.parseBoolean(session.conf().get("obs.fs.bucket.enable", "true"))) {
          obsClient.newBucket(new NewBucketRequest(bucketName, regionName));
        } else {
          obsClient.createBucket(bucketName, regionName);
        }
      }
    }
  }

  public static void deleteBucket(String bucketName, Boolean ifExists, SparkSession session)
      throws IOException {
    try (ObsClient obsClient = createObsClient(session)) {
      if (obsClient.headBucket(bucketName)) {
        obsClient.deleteBucket(bucketName);
      }
    }
  }

  public static ObsClient createObsClient(SparkSession session) {

    String endpoint = session.conf().get(OBSSparkConstants.END_POINT);
    String ak = session.conf().get(OBSSparkConstants.AK);
    String sk = session.conf().get(OBSSparkConstants.SK);
    ObsConfiguration config = new ObsConfiguration();
    config.setEndPoint(endpoint);

    return new ObsClient(ak, sk, config);
  }

  /**
   * get bucket name
   * path like s3n://docker-test3/input, bucket name is docker-test3
   *
   * @param path
   * @return
   */
  public static String getBucketName(String path) {
    if (null == path || path.isEmpty()) {
      return "";
    }

    if (path.startsWith("s3n") || path.startsWith("s3a") || path.startsWith("obs")) {
      String[] arrays = path.split("/");
      if (arrays.length >= 3) {
        return arrays[2];
      }
    }

    return "";
  }

  public static void createDir(String bucket, String dirKey, SparkSession session)
      throws IOException {
    //dirKey ends with "/" will be dir, otherwise obj.
    try (ObsClient obsClient = createObsClient(session)) {
      PutObjectRequest poq = new PutObjectRequest();
      poq.setBucketName(bucket);
      poq.setObjectKey(dirKey);
      obsClient.putObject(poq);
    }
  }

  public static List<String[]> readObsFileByPagesCsvFormat(String path, SparkSession session,
      int startLineNum, int limit) throws IOException {
    ObsObject obsObject = null;
    InputStream is = null;
    InputStreamReader ir = null;
    LineNumberReader input = null;
    LineIterator lineIterator = null;

    List<String[]> result = new LinkedList<String[]>();
    try (ObsClient obsClient = OBSUtil.createObsClient(session)) {
      String bucketName = OBSUtil.getBucketName(path);
      String tempPath =
          path.substring(path.indexOf(bucketName) + bucketName.length(), path.length());
      while (tempPath.startsWith("/")) {
        tempPath = tempPath.substring(1, tempPath.length());
      }

      ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
      listObjectsRequest.setBucketName(bucketName);
      listObjectsRequest.setPrefix(tempPath + "/");
      listObjectsRequest.setDelimiter("/");
      ObjectListing objListing = obsClient.listObjects(listObjectsRequest);
      String csvFileKey = null;
      for (ObsObject obsObject1 : objListing.getObjects()) {
        if (obsObject1.getObjectKey().endsWith(".csv")) {
          csvFileKey = obsObject1.getObjectKey();
          break;
        }
      }

      //get the csv file content.
      obsObject = obsClient.getObject(bucketName, csvFileKey, null);
      is = obsObject.getObjectContent();
      ir = new InputStreamReader(is, "UTF-8");
      input = new LineNumberReader(ir);
      lineIterator = new LineIterator(input);
      Configuration config = new Configuration();
      // TODO: set delimiter into config according to the params in extractCsvParserSettings
      CsvParserSettings settings = CSVInputFormat.extractCsvParserSettings(config);
      CsvParser csvParser = new CsvParser(settings);
      String line;
      int currentLineNum = 0;
      int count = 0;
      while (lineIterator.hasNext()) {
        line = lineIterator.next();
        if (currentLineNum >= startLineNum && currentLineNum < startLineNum + limit) {
          String[] row = null;
          try {
            row = csvParser.parseLine(line);
          } catch (Exception e) {
            LOGGER.error("Bad line found in csv file: ", e);
            continue;
          }
          result.add(row);
          count++;
          currentLineNum++;
        } else if (currentLineNum >= startLineNum + limit) {
          //enough
          break;
        } else {
          currentLineNum++;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to read csv result page:", e);
      throw new IOException("Failed to read csv result page");
    } finally {
      try {
        if (null != is) is.close();
        if (null != ir) ir.close();
        if (null != input) input.close();
        if (obsObject != null && null != obsObject.getObjectContent()) {
          obsObject.getObjectContent().close();
        }
        LineIterator.closeQuietly(lineIterator);
      } catch (IOException e) {
        LOGGER.error("Fail to close stream. Error message: " + e.getMessage());
      }
    }

    return result;
  }

  public static List<String> listFiles(String path, LoginRequestManager.Credential credential) {
    ObsClient obsClient = getObsClient(credential);
    String[] obsBucketAndPath = getObsBucketAndPath(path);
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(obsBucketAndPath[0]);
    if (obsBucketAndPath[1] != null) {
      listObjectsRequest.setPrefix(obsBucketAndPath[1]);
    }
    ObjectListing listing = obsClient.listObjects(listObjectsRequest);
    List<String> paths = new ArrayList<>();
    for (ObsObject object : listing.getObjects()) {
      paths.add(object.getObjectKey());
    }
    return paths;
  }

  private static String[] getObsBucketAndPath(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    }
    int endIndex = path.indexOf("/");
    if (endIndex < 0) {
      endIndex = path.length();
    }
    String[] paths = new String[2];
    paths[0] = path.substring(0, endIndex);
    if (endIndex < path.length()) {
      paths[1] = path.substring(endIndex + 1, path.length());
    }
    return paths;
  }

  private static ObsClient getObsClient(LoginRequestManager.Credential credential) {
    ObsConfiguration config = new ObsConfiguration();
    config.setSocketTimeout(30000);
    config.setConnectionTimeout(10000);
    config.setEndPoint(RestConstants.OBS_ENDPOINT);
    return new ObsClient(credential.getAccess(), credential.getSecret(),
        credential.getSecuritytoken(), config);
  }

  public static String getObjectinString(String path, String objectKey,
      LoginRequestManager.Credential credential) throws IOException {
    ObsClient obsClient = getObsClient(credential);
    String[] obsBucketAndPath = getObsBucketAndPath(path);
    ObsObject object = obsClient.getObject(obsBucketAndPath[0], objectKey);
    BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
    StringBuilder builder = new StringBuilder();
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      builder.append(line).append("\n");
    }
    return builder.toString();
  }

  /**
   * Copy file(s) from OBS to local recursively
   * <p>
   * If path ends with "/", it will be treated as a directory and the files (only) from that directory will be copied.
   * Else, only one file represented by path will be copied.
   * <p>
   * Usage: OBSUtil.copyToLocal("obs://leo-query-data-user-cn-north-7/data/images/", "/tmp/mydir", session, true);
   *
   * @param path      like s3n://docker-test3/input
   * @param destDir   local directory
   * @param session
   * @param recursive
   */
  public static void copyToLocal(String path, String destDir, SparkSession session, boolean recursive)
      throws IOException {
    String bucket = getBucketName(path);
    String obsPath = path.substring(path.indexOf(bucket) + bucket.length());
    while (obsPath.startsWith("/")) {
      obsPath = obsPath.substring(1);
    }

    destDir = (destDir.endsWith("/")) ? destDir : destDir + "/";
    File localDir = new File(destDir);
    if (!localDir.exists() && !localDir.mkdirs()) {
      throw new RuntimeException("Error creating directory " + localDir.getParentFile().toString());
    }

    try (ObsClient obsClient = createObsClient(session)) {
      if (!path.endsWith("/")) {
        // Only one file to copy
        ObsObject obsObject = obsClient.getObject(bucket, obsPath);

        String fileName = obsPath.substring(obsPath.lastIndexOf("/") + 1);
        File localFile = new File(destDir + fileName);
        IOUtils.copy(obsObject.getObjectContent(), new FileOutputStream(localFile));
      } else {
        // Copy all files and directories in path
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        listObjectsRequest.setPrefix(obsPath);
        listObjectsRequest.setDelimiter("/");
        ObjectListing objListing = obsClient.listObjects(listObjectsRequest);

        // Copy all files from OBS path
        for (ObsObject obsObject : objListing.getObjects()) {
          if (obsObject.getObjectKey().endsWith("/")) {
            // objListing.getObjects() also returns an `ObsObject` instance with parent directory path.
            // Just ignore that
            continue;
          }
          String fileName = obsObject.getObjectKey().substring(obsObject.getObjectKey().lastIndexOf("/") + 1);
          File localFile = new File(destDir + fileName);
          IOUtils.copy(obsClient.getObject(bucket, obsObject.getObjectKey()).getObjectContent(), new FileOutputStream(localFile));
        }

        // Recursively copy all the directories from path
        if (recursive) {
          for (String dir : objListing.getCommonPrefixes()) {
            String dirName = dir.substring(0, dir.length() - 1);
            dirName = dirName.substring(dirName.lastIndexOf("/") + 1);
            if (!new File(destDir + dirName).mkdirs()) {
              throw new RuntimeException("Error creating directory " + localDir.getParentFile().toString());
            }
            copyToLocal(path + dirName + "/", destDir + dirName + "/", session, true);
          }
        }
      }
    }
  }

}
