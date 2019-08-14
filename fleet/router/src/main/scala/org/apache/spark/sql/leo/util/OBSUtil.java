package org.apache.spark.sql.leo.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.LinkedList;
import java.util.List;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.fs.NewBucketRequest;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.carbondata.SparkS3Constants;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat;

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

    String endpoint = session.conf().get(SparkS3Constants.END_POINT);
    String ak = session.conf().get(SparkS3Constants.AK);
    String sk = session.conf().get(SparkS3Constants.SK);
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

}
