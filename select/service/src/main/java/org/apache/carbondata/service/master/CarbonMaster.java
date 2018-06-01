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

package org.apache.carbondata.service.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.service.common.ServiceUtil;
import org.apache.carbondata.store.CarbonRowReadSupport;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

public class CarbonMaster {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonMaster.class.getName());

  private static final List<ServerInfo> serverList = new ArrayList<ServerInfo>();

  private static final Map<Table, ServerInfo> dataCacheMap = new HashMap<Table, ServerInfo>();

  public static void init(VisionConfiguration conf) {
     ServiceUtil.parserServerList(conf.serverList(), serverList);
  }

  public static List<InputSplit> getSplit(CarbonTable table, Expression filter)
      throws VisionException {
    try {
      final CarbonTableInputFormat format = new CarbonTableInputFormat();
      final Job job = new Job(new Configuration());
      CarbonInputFormat.setTableInfo(job.getConfiguration(), table.getTableInfo());
      CarbonInputFormat.setTablePath(job.getConfiguration(), table.getTablePath());
      CarbonInputFormat.setTableName(job.getConfiguration(), table.getTableName());
      CarbonInputFormat.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
      CarbonInputFormat.setCarbonReadSupport(job.getConfiguration(), CarbonRowReadSupport.class);

      if (filter != null) {
        CarbonInputFormat.setFilterPredicates(job.getConfiguration(), filter);
      }

      return format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));
    } catch (IOException e) {
      String message = "Failed to getSplit";
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  public static List<ServerInfo> serverList() {
    return serverList;
  }

  public static synchronized void addCacheInfo(Table table, ServerInfo serverInfo) {
    dataCacheMap.put(table, serverInfo);
  }

  public static ServerInfo getCacheInfo(Table table) {
    return dataCacheMap.get(table);
  }

}
