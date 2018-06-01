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

package org.apache.carbondata.service.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.service.master.CarbonMaster;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.service.service.PredictServiceFactory;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

public class CarbonScheduler {

  private VisionConfiguration conf;

  private Map<ServerInfo, ServicePool> serviceMap;

  private AtomicLong cacheCount;

  private int coreNum;

  public CarbonScheduler(VisionConfiguration conf) {
    this.conf = conf;
    serviceMap = new HashMap<ServerInfo, ServicePool>();
    cacheCount = new AtomicLong(0);
    this.coreNum = 1;
  }

  public void init() throws VisionException {
    if (CarbonMaster.serverList().isEmpty()) {
      throw new VisionException("There is no server. Please check the configuration");
    }
    coreNum = conf.scheduleCoreNum();
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      List<PredictService> services = new ArrayList<>(coreNum);
      for (int i = 0; i < coreNum; i++) {
        services.add(PredictServiceFactory.getPredictService(serverInfo));
      }
      serviceMap.put(serverInfo, new ServicePool(services));
    }
  }

  public boolean loadLibrary(String libName) {
    boolean status = true;
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      status = status && serviceMap.get(serverInfo).get().loadLibrary(libName);
    }
    return status;
  }

  public Model loadModel(String modelPath) throws VisionException {
    Model model = null;
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      model = serviceMap.get(serverInfo).get().loadModel(modelPath);
    }
    return model;
  }

  public byte[] cacheTable(Table table, int cacheLevel) throws VisionException {
    int serverCount = CarbonMaster.serverList().size();
    int serverIndex = (int) (cacheCount.getAndIncrement() % serverCount);
    ServerInfo chooseServer = CarbonMaster.serverList().get(serverIndex);
    byte[] serializedTable = null;
    try {
      serializedTable = serviceMap.get(chooseServer).get().cacheTable(table, cacheLevel);
      return serializedTable;
    } finally {
      if (serializedTable != null) {
        CarbonMaster.addCacheInfo(table, chooseServer);
      }
    }
  }

  public Record[] search(CarbonMultiBlockSplit split, PredictContext context)
      throws VisionException {
    ServerInfo chooseServer = CarbonMaster.getCacheInfo(context.getTable());

    if (chooseServer == null) {
      int serverCount = CarbonMaster.serverList().size();
      int serverIndex = (int) (cacheCount.getAndIncrement() % serverCount);
      chooseServer = CarbonMaster.serverList().get(serverIndex);
    }

    return serviceMap.get(chooseServer).get().search(split, context);
  }

}
