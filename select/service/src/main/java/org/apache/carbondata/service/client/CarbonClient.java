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

package org.apache.carbondata.service.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.common.ServiceUtil;
import org.apache.carbondata.service.master.CarbonMaster;
import org.apache.carbondata.service.schedule.CarbonScheduler;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.common.VisionUtil;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.mapreduce.InputSplit;

public class CarbonClient {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonClient.class.getName());

  private VisionConfiguration conf;
  private Map<Table, CarbonTable> cache = new HashMap<>();
  private CarbonScheduler scheduler;

  public CarbonClient(VisionConfiguration conf) {
    this.conf = conf;
  }

  public void init() throws VisionException {
    CarbonMaster.init(conf);
    scheduler = new CarbonScheduler(conf);
    scheduler.init();
  }

  public boolean loadLibrary(String libName) {
    return scheduler.loadLibrary(libName);
  }

  public Model loadModel(String modelPath) throws VisionException {
    return scheduler.loadModel(modelPath);
  }

  public CarbonTable cacheTable(Table table, boolean allNode) throws VisionException {
    try {
      // CacheLevel Disk
      TableInfo tableInfo = TableInfo.deserialize(
          scheduler.cacheTable(table, CacheLevel.Disk.getIndex(), allNode, cache.keySet()));
      CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      // cache metadata and index
      CarbonMaster.getSplit(carbonTable, null);
      cache.put(table, carbonTable);
      return carbonTable;
    } catch (IOException e) {
      String message = "Failed to cache table: " + table.getPresentName();
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  public Record[] search(PredictContext context) throws VisionException {
    long t1 = System.currentTimeMillis();
    CarbonTable carbonTable = cache.get(context.getTable());
    if (carbonTable == null) {
      LOGGER.audit("need cache table at first");
      carbonTable = cacheTable(context.getTable(), false);
    }
    List<InputSplit> splits = CarbonMaster.getSplit(carbonTable, null);

    long t2 = System.currentTimeMillis();
    Record[] result = scheduler.search(new CarbonMultiBlockSplit(splits), context);
    ServiceUtil.sortRecords(result, context.getConf().projection().length);

    long t3 = System.currentTimeMillis();
    LOGGER.audit("Search taken time: " + (t3 - t1) + " ms, " +
        VisionUtil.printlnTime(t1, t2, t3));
    return result;
  }

}
