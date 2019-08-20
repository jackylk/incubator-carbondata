package org.apache.spark.sql.leo.util.obs;

import com.huawei.cloud.obs.OBSSparkConstants;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class OBSUtil {

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


  public static ObsClient createObsClient(SparkSession session) {
    String endpoint = session.conf().get(OBSSparkConstants.END_POINT);
    String ak = session.conf().get(OBSSparkConstants.AK);
    String sk = session.conf().get(OBSSparkConstants.SK);

    ObsConfiguration config = new ObsConfiguration();
    config.setEndPoint(endpoint);

    return new ObsClient(ak, sk, config);
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
