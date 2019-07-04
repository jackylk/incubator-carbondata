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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.huawei.cloud.RestConstants;
import com.huawei.cloud.credential.LoginRequestManager;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;

public class OBSUtil {

  public static List<String> listFiles(String path, LoginRequestManager.Credential credential)
      throws Exception {
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
      LoginRequestManager.Credential credential) throws Exception {
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

}
