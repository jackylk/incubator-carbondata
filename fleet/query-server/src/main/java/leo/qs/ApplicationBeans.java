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

package leo.qs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import org.apache.carbondata.common.logging.LogServiceFactory;

import leo.job.JobMetaStoreClient;
import leo.qs.client.JobMetaStoreClientImpl;
import leo.qs.client.RestTemplateClient;
import leo.qs.locator.LocalRunnerLocator;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration public class ApplicationBeans {
  private static Logger LOGGER =
      LogServiceFactory.getLogService(RestTemplateClient.class.getCanonicalName());

  @Resource ApplicationProperties applicationProperties;

  @Bean public ClientHttpRequestFactory simpleClientHttpRequestFactory() {
    HttpComponentsClientHttpRequestFactory factory =
        new HttpComponentsClientHttpRequestFactory(httpClient());
    return factory;
  }

  @Bean public RestTemplate restTemplate(@Autowired ClientHttpRequestFactory factory) {
    LOGGER.info("restTemplate is created.");
    return new RestTemplate(factory);
  }

  @Bean public HttpClient httpClient() {
    // 长连接保持30秒
    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(30, TimeUnit.SECONDS);
    //设置整个连接池最大连接数 根据自己的场景决定
    connectionManager.setMaxTotal(500);
    //同路由的并发数,路由是对maxTotal的细分
    connectionManager.setDefaultMaxPerRoute(500);

    //requestConfig
    RequestConfig requestConfig = RequestConfig.custom()
        //服务器返回数据(response)的时间，超过该时间抛出read timeout
        .setSocketTimeout(10000)
        //连接上服务器(握手成功)的时间，超出该时间抛出connect timeout
        .setConnectTimeout(5000)
        //从连接池中获取连接的超时时间
        .setConnectionRequestTimeout(500).build();
    //headers
    List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader("Content-type", "application/json;charset=UTF-8"));

    return HttpClientBuilder.create().setDefaultRequestConfig(requestConfig)
        .setConnectionManager(connectionManager).setDefaultHeaders(headers)
        // 保持长连接配置，需要在头添加Keep-Alive
        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
        //重试次数，默认是3次，没有开启
        .setRetryHandler(new DefaultHttpRequestRetryHandler(2, true)).build();
  }

  @Bean public String leoVersion() {
    LOGGER.info("leo.version=" + applicationProperties.getLeoVersion());
    return applicationProperties.getLeoVersion();
  }

  @Bean public JobMetaStoreClient metaStoreClient() {
    LOGGER.info("metaStoreClient init.");
    JobMetaStoreClientImpl metaStoreClient = new JobMetaStoreClientImpl();
    boolean isMetaClientMock = applicationProperties.getMockMetaStore();
    if (isMetaClientMock) {
      ((JobMetaStoreClientImpl) metaStoreClient).setMock(isMetaClientMock);
      ((JobMetaStoreClientImpl) metaStoreClient).setEndPoint("http://localhost:8080");
    } else {
      ((JobMetaStoreClientImpl) metaStoreClient)
          .setEndPoint(Main.getSession().conf().get("leo.metastore.endpoint"));
    }
    LOGGER.info("isMetaClientMock： " + isMetaClientMock);

    return metaStoreClient;
  }

  @Bean
  public LocalRunnerLocator localRunnerLocator(@Autowired JobMetaStoreClient metaStoreClient) {
    LocalRunnerLocator runnerLocator = new LocalRunnerLocator(metaStoreClient);
    return runnerLocator;
  }
}
