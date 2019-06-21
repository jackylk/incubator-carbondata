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

package org.apache.leo.model.rest;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.leo.model.rest.RestConstants.*;

/**
 * Helps to login to Huawei cloud.
 */
public class LoginRequestManager implements Serializable {

  public static LoginInfo login(String username, String password, OkHttpClient client)
      throws Exception {
    String loginJson = "{\n" +
        "  \"auth\": {\n" +
        "    \"identity\": {\n" +
        "      \"methods\": [\"password\"],\n" +
        "      \"password\": {\n" +
        "        \"user\": {\n" +
        "          \"name\": \"" + username + "\",\n" +
        "          \"password\": \"" + password + "\",\n" +
        "          \"domain\": {\n" +
        "            \"name\": \"" + username + "\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"scope\": {\n" +
        "      \"project\": {\n" +
        "        \"name\": \"cn-north-1\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";
    LoginInfo loginInfo = new LoginInfo();
    Exception[] exception = new Exception[1];
    RestUtil.postAsync(HUAWEI_CLOUD_AUTH_ENDPOINT, loginJson,
        new Callback() {
          @Override public void onFailure(Call call, IOException e) {
            exception[0] = e;
          }

          @Override public void onResponse(Call call, Response response) throws IOException {
            if (response.isSuccessful()) {
              ObjectMapper objectMapper = new ObjectMapper();
              Map<String, Object> jsonNodeMap = null;
              try {
                jsonNodeMap = objectMapper
                    .readValue(response.body().string(), new TypeReference<Map<String, Object>>() {
                    });
              } catch (Exception e) {
                exception[0] = e;
              }
              loginInfo.setUserName(username);
              loginInfo.setLoggedIn(true);
              loginInfo.setToken(response.header(AUTH_TOKEN_HEADER));
              loginInfo.setProjectId(
                  ((Map) ((Map) jsonNodeMap.get("token")).get("project")).get("id").toString());
            }
          }
        }, client);
    while (exception[0] == null && !loginInfo.isLoggedIn()) {
      Thread.sleep(10);
    }
    if (exception[0] != null) {
      throw exception[0];
    }
    return loginInfo;
  }

  public static class LoginInfo {
    private boolean loggedIn = false;
    private String token;
    private String projectId;
    private String userName;

    public String getToken() {
      return token;
    }

    public void setToken(String token) {
      this.token = token;
    }

    public String getProjectId() {
      return projectId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public String getUserName() {
      return userName;
    }

    public void setUserName(String userName) {
      this.userName = userName;
    }

    public boolean isLoggedIn() {
      return loggedIn;
    }

    public void setLoggedIn(boolean loggedIn) {
      this.loggedIn = loggedIn;
    }
  }
}
