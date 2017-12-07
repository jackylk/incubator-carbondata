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

package org.apache.carbondata.spark.acl.filesystem;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.carbondata.spark.acl.CarbonUserGroupInformation;

/**
 * This wrapper class is used to execute privileged operations
 * as  IUG doAs proxy user . It will use proxy user only if current user is not same
 * as login user. Otherwise directly execute the operations.
 */
public class PrivilegedFileOperation {
  public static <T> T execute(PrivilegedExceptionAction<T> pea)
      throws IOException, InterruptedException {
    String loginUser = CarbonUserGroupInformation.getInstance().getLoginUser().getUserName();
    String currentUser = CarbonUserGroupInformation.getInstance().getCurrentUser().getUserName();
    if ((null != currentUser) && (!currentUser.equals(loginUser))) {
      return CarbonUserGroupInformation.getInstance().getCurrentUser().doAs(pea);
    } else {
      try {
        return pea.run();
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    }
  }
}
