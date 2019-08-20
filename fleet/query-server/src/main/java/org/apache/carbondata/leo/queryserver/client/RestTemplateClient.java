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

package org.apache.carbondata.leo.queryserver.client;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class RestTemplateClient {
  private static Logger LOGGER =
      LogServiceFactory.getLogService(RestTemplateClient.class.getCanonicalName());
  @Autowired
  RestTemplate restTemplate;

  public <T> T get(String url, Class<T> responseType) {
    LOGGER.info(url + " is called.");
    return restTemplate.getForEntity(url, responseType).getBody();
  }

  public <T> String post(String url, HttpEntity<T> request) {
    LOGGER.info(url + " is called.");
    return restTemplate.exchange(url, HttpMethod.POST, request, String.class).getBody();
  }

  public <T> String put(String url, HttpEntity<T> request) {
    LOGGER.info(url + " is called.");
    return restTemplate.exchange(url, HttpMethod.PUT, request, String.class).getBody();
  }

  public <T> String delete(String url) {
    LOGGER.info(url + " is called.");
    return restTemplate.exchange(url, HttpMethod.DELETE, null, String.class).getBody();
  }

}
