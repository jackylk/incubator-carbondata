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

import org.apache.hadoop.security.UserGroupInformation;

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

  public UserGroupInformation getCurrentUser() throws IOException {
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    if (isDriver && carbonSessionInfo != null) {
      try {
        UserGroupInformation userUniqueUGIObject =
            (UserGroupInformation) carbonSessionInfo.getNonSerializableExtraInfo()
                .get(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT);
        String userName = (String) carbonSessionInfo.getNonSerializableExtraInfo()
            .get(CarbonInternalCommonConstants.USER_NAME);
        if (null == userUniqueUGIObject && userName != null && !userName
            .equals(UserGroupInformation.getCurrentUser().getShortUserName())) {
          UserGroupInformation proxyUser =
              UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
          // set user unique object only for the first time so that for the same user in the current
          // thread it is cached and returned same for all the calls for the current query
          carbonSessionInfo.getNonSerializableExtraInfo()
              .put(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT, proxyUser);
          LOGGER.info("user UGI object created: " + proxyUser.hashCode());
          return proxyUser;
        } else if (null != userUniqueUGIObject) {
          return userUniqueUGIObject;
        }
        return UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        LOGGER.error(e, e.getMessage());
        return UserGroupInformation.getCurrentUser();
      }
    } else {
      return UserGroupInformation.getCurrentUser();
    }
  }

  public UserGroupInformation getLoginUser() throws IOException {
    return UserGroupInformation.getLoginUser();
  }

  /**
   * This method will clean up all the UGI objects which will other wise be cached and
   * can lead to memory leak over a long run
   */
  public void cleanUpUGIFromCurrentThread() {
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    UserGroupInformation userUniqueUGIObject =
        (UserGroupInformation) carbonSessionInfo.getNonSerializableExtraInfo()
            .get(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT);
    if (isDriver && null != userUniqueUGIObject) {
      // set the info object reference to null in the current thread
      carbonSessionInfo.getNonSerializableExtraInfo()
          .put(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT, null);
    }
  }
}
