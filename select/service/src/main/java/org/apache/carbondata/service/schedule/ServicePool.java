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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.service.service.PredictService;

public class ServicePool {

  List<PredictService> services;

  AtomicLong count;

  public ServicePool(List<PredictService> services) {
    this.services = services;
    count = new AtomicLong(0);
  }

  public PredictService get() {
    return services.get((int)(count.getAndIncrement() % services.size()));
  }
}
