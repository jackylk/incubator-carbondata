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

package org.apache.carbondata.service.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.vision.common.VisionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class PredictServiceFactory {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(PredictServiceFactory.class.getName());

  public static PredictService getPredictService(ServerInfo serverInfo) throws VisionException {
    InetSocketAddress address;
    try {
      address = new InetSocketAddress(
          InetAddress.getByName(serverInfo.getHost()),
          serverInfo.getPort());
    } catch (UnknownHostException e) {
      String message = "unknown server  " + serverInfo;
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
    try {
      return RPC
          .getProxy(PredictService.class, PredictService.versionID, address, new Configuration());
    } catch (IOException e) {
      String message = "Failed to get PredictService";
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

}
