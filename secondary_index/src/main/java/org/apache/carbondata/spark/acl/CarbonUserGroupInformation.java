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

import org.apache.hadoop.security.UserGroupInformation;

public class CarbonUserGroupInformation {
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
    try {
      if (isDriver && ThreadLocalUsername.getUsername() != null && !ThreadLocalUsername
          .getUsername().equals(UserGroupInformation.getCurrentUser().getShortUserName())) {
        return UserGroupInformation.createProxyUser(ThreadLocalUsername.getUsername(),
            UserGroupInformation.getLoginUser());
      }
      return UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      return UserGroupInformation.getCurrentUser();
    }
  }

  public UserGroupInformation getLoginUser() throws IOException {
    return UserGroupInformation.getLoginUser();
  }
}
