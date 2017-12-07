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
