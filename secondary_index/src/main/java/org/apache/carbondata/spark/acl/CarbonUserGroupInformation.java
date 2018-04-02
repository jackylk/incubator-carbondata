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

package org.apache.carbondata.spark.acl;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

import org.apache.carbondata.spark.core.CarbonInternalCommonConstants;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.CarbonEnv;
import org.apache.spark.sql.SparkSession;

public class CarbonUserGroupInformation {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonUserGroupInformation.class.getName());

  private boolean isDriver = false;

  private static final CarbonUserGroupInformation CARBONUGINSTANCE =
      new CarbonUserGroupInformation();

  private CarbonUserGroupInformation() {
  }

  public static CarbonUserGroupInformation getInstance() {
    return CARBONUGINSTANCE;
  }

  public void enableDriverUser() throws IOException {
    isDriver = true;
  }

  /**
   * Create and return a proxy user if required. Return null otherwise.
   */
  public UserGroupInformation createCurrentUser(String userName) throws IOException {

    UserGroupInformation proxyUser = null;
    try {

      if (userName != null && !userName
          .equals(UserGroupInformation.getCurrentUser().getShortUserName())) {
        proxyUser =
            UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());

        LOGGER.info("Proxy UGI object created: " + proxyUser.hashCode());
      }
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }

    return proxyUser;
  }

  public UserGroupInformation getCurrentUser() throws IOException {
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();

    UserGroupInformation userUniqueUGIObject = null;
    if (isDriver && carbonSessionInfo != null) {
      userUniqueUGIObject = (UserGroupInformation) carbonSessionInfo.getNonSerializableExtraInfo()
          .get(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT);
    }

    if (null == userUniqueUGIObject) {
      userUniqueUGIObject = UserGroupInformation.getCurrentUser();
    }

    return userUniqueUGIObject;
  }

  public UserGroupInformation getLoginUser() throws IOException {
    return UserGroupInformation.getLoginUser();
  }

  /**
   * This method will clean up all the UGI objects which will other wise be cached and
   * can lead to memory leak over a long run
   */
  public static void cleanUpUGIFromSession(SparkSession sparkSession) {

    CarbonSessionInfo carbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo();
    CarbonEnv.carbonEnvMap().remove(sparkSession);
    UserGroupInformation userUniqueUGIObject =
        (UserGroupInformation) carbonSessionInfo.getNonSerializableExtraInfo()
            .remove(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT);

    if (null == userUniqueUGIObject) {
      LOGGER.info("No Proxy UGI found in session info.");
    } else {
      try {
        FileSystem.closeAllForUGI(userUniqueUGIObject);
        LOGGER.info(
            "Proxy UGI found in cache. Cleaned the FileSystem cache for ugi " + userUniqueUGIObject
                .hashCode());
      } catch (Exception e) {
        LOGGER.error(e, " Error in closing file System.");
      }
    }
  }
}
