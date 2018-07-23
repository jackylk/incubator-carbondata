/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
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
