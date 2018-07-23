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
package org.apache.carbondata.spark.acl;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.tools.GetGroups;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * User groups utility
 */
public class UserGroupUtils {

  /**
   * returns current user groups using UgmProtocol from hdfs
   *
   * @param conf
   * @return
   * @throws IOException
   */
  public static String[] getGroupsForUserFromNameNode(Configuration conf, URI uri)
      throws IOException {
    GetGroups groups = new GetGroups(conf);
    // add all necessary default configurations using set conf
    groups.setConf(conf);
    return getUgmProtocol(groups.getConf(), uri).getGroupsForUser(
        CarbonUserGroupInformation.getInstance().getCurrentUser().getShortUserName());
  }

  /**
   * returns protocol which maps users to groups
   *
   * @param conf uri
   * @return
   * @throws IOException
   */
  private static GetUserMappingsProtocol getUgmProtocol(Configuration conf, URI uri)
      throws IOException {
    return NameNodeProxies.createProxy(conf, uri, GetUserMappingsProtocol.class).getProxy();
  }

}
