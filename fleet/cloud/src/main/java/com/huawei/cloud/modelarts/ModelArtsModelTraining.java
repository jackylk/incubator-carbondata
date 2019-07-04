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

package com.huawei.cloud.modelarts;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.ai.DataScan;
import org.apache.carbondata.ai.ModelTrainingAPI;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import com.google.gson.Gson;
import com.huawei.cloud.credential.LoginRequestManager;
import com.huawei.cloud.obs.OBSUtil;
import com.huawei.cloud.util.RestUtil;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import static com.huawei.cloud.RestConstants.MODELARTS_CN_NORTH_V1_ENDPOINT;
import static com.huawei.cloud.RestConstants.MODELARTS_MODEL;
import static com.huawei.cloud.RestConstants.MODELARTS_TRAINING_REST;
import static com.huawei.cloud.RestConstants.MODELARTS_TRAINING_VERSIONS;
import static com.huawei.cloud.RestConstants.SEPARATOR;

/**
 * It creates the training job in Model Arts
 */
public class ModelArtsModelTraining implements ModelTrainingAPI {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ModelArtsModelTraining.class.getName());

  private static OkHttpClient client =
      new OkHttpClient().newBuilder().readTimeout(60, TimeUnit.SECONDS).build();

  private static Map<String, LoginRequestManager.LoginInfo> loginInfoMap = new HashMap<>();

  private static Map<LoginRequestManager.LoginInfo, LoginRequestManager.Credential> credentialMap =
      new HashMap<>();

  /**
   * It creates training job in modelarts and starts it to generate model.
   */
  @Override
  public long startTrainingJob(Map<String, String> options, String expName,
      DataScan dataScan) {
    // Start a new training job in ModelArts
    String json = ModelArtsTrainJobVO.generateJson(options, expName, dataScan);
    LOGGER.info(json);
    LoginRequestManager.LoginInfo loginInfo = getLoginInfo();

    Object[] status = new Object[2];
    RestUtil.postAsync(MODELARTS_CN_NORTH_V1_ENDPOINT + loginInfo.getProjectId() + SEPARATOR
        + MODELARTS_TRAINING_REST, json, new Callback() {
      @Override public void onFailure(Call call, IOException e) {
        status[0] = e;
      }

      @Override public void onResponse(Call call, Response response) throws IOException {
        if (response.isSuccessful()) {
          ObjectMapper objectMapper = new ObjectMapper();
          Map<String, Object> jsonNodeMap = null;
          try {
            String rspStr = response.body().string();
            LOGGER.info(rspStr);
            jsonNodeMap = objectMapper.readValue(rspStr, new TypeReference<Map<String, Object>>() {
            });
            if ((Boolean) jsonNodeMap.get("is_success")) {
              status[1] = jsonNodeMap.get("job_id");
            } else {
              status[0] = new Exception(rspStr);
            }
          } catch (Exception e) {
            LOGGER.error(e);
            status[0] = e;
          }
        } else {
          status[0] = new Exception(response.body().string());
        }
      }
    }, loginInfo.getToken(), client);
    while (status[0] == null && status[1] == null) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    if (status[0] != null) {
      throw new RuntimeException((Exception) status[0]);
    }
    return Long.parseLong(status[1].toString());
  }

  private static LoginRequestManager.LoginInfo getLoginInfo() {
    String maUserName = CarbonProperties.getInstance().getProperty("leo.ma.username");
    String maPwd = CarbonProperties.getInstance().getProperty("leo.ma.password");
    if (maUserName == null || maPwd == null) {
      throw new RuntimeException("User name and password should be set in carbon properties");
    }
    LoginRequestManager.LoginInfo loginInfo = loginInfoMap.get(maUserName);
    if (loginInfo == null) {
      loginInfo = LoginRequestManager.login(maUserName, maPwd, client);
      loginInfoMap.put(maUserName, loginInfo);
    }
    return loginInfo;
  }

  private static LoginRequestManager.Credential getCredental(
      LoginRequestManager.LoginInfo loginInfo) throws Exception {
    LoginRequestManager.Credential credential = credentialMap.get(loginInfo);
    if (credential == null) {

      credential = LoginRequestManager.getTemporaryAccessKeys(loginInfo, client);
      credentialMap.put(loginInfo, credential);
    }
    return credential;
  }

  @Override
  public void stopTrainingJob(long jobId) throws IOException {
    // stop and delete the training job in ModelArts
    LoginRequestManager.LoginInfo loginInfo = getLoginInfo();
    Response response = RestUtil.delete(
        MODELARTS_CN_NORTH_V1_ENDPOINT + loginInfo.getProjectId() + SEPARATOR
            + MODELARTS_TRAINING_REST + SEPARATOR + jobId, loginInfo.getToken(), client);
  }

  @Override public Map<String, String> getTrainingJobInfo(long jobId) throws Exception {
    // stop and delete the training job in ModelArts
    LoginRequestManager.LoginInfo loginInfo = getLoginInfo();
    Response response = RestUtil.get(
        MODELARTS_CN_NORTH_V1_ENDPOINT + loginInfo.getProjectId() + SEPARATOR
            + MODELARTS_TRAINING_REST + SEPARATOR + jobId + SEPARATOR + MODELARTS_TRAINING_VERSIONS,
        loginInfo.getToken(), client);
    if (response.isSuccessful()) {
      ObjectMapper objectMapper = new ObjectMapper();
      String rspStr = response.body().string();
      LOGGER.info(rspStr);
      Map<String, Object> jsonNodeMap =
          objectMapper.readValue(rspStr, new TypeReference<Map<String, Object>>() {
          });
      Map<String, String> jobDetail = new HashMap<>();

      Object versionsObj = jsonNodeMap.get("versions");
      List<LinkedHashMap> versions = (List) versionsObj;
      jobDetail
          .put("status", ModelArtsStatusCodes.getStatus(versions.get(0).get("status").toString()));
      jobDetail.put("duration", versions.get(0).get("duration").toString());
      jobDetail.put("json", rspStr);
      return jobDetail;
    } else {
      throw new Exception("Training job retrieval failed" + response.body().string());
    }
  }

  @Override
  public String importModel(Map<String, String> options, String udfName) throws Exception {
    String train_url = options.get("train_url");
    LoginRequestManager.LoginInfo loginInfo = getLoginInfo();
    LoginRequestManager.Credential credential = getCredental(loginInfo);
    List<String> listFiles = OBSUtil.listFiles(train_url, credential);
    String configFile = null;
    for (String listFile : listFiles) {
      if (listFile.contains("config.json")) {
        configFile = listFile;
        break;
      }
    }
    if (configFile == null) {
      throw new UnsupportedOperationException(
          "config.json must be present in model location" + train_url);
    }
    String objectinString = OBSUtil.getObjectinString(train_url, configFile, credential);

    String executionCode = null;
    for (String listFile : listFiles) {
      if (listFile.contains("customize_service.py")) {
        executionCode = train_url + "/model/customize_service.py";
        break;
      }
    }
    Gson gson = new Gson();
    ModelArtsImportModelVO.Config config =
        gson.fromJson(objectinString, ModelArtsImportModelVO.Config.class);

    String json = ModelArtsImportModelVO.generateJSON(options, udfName, config, executionCode);
    LOGGER.info(json);
    Response response = RestUtil.postSync(
        MODELARTS_CN_NORTH_V1_ENDPOINT + loginInfo.getProjectId() + SEPARATOR + MODELARTS_MODEL,
        json, loginInfo.getToken(), client);
    if (response.isSuccessful()) {
      ObjectMapper objectMapper = new ObjectMapper();
      String rspStr = response.body().string();
      LOGGER.info(rspStr);
      Map<String, Object> jsonNodeMap =
          objectMapper.readValue(rspStr, new TypeReference<Map<String, Object>>() {
          });
      return jsonNodeMap.get("model_id").toString();
    } else {
      throw new Exception(
          "Importing model failed with error code: " + response.code() + " with response: "
              + response.body().string());
    }
  }

  @Override public Map<String, String> getModelInfo(String modelId) throws Exception {
    LoginRequestManager.LoginInfo loginInfo = getLoginInfo();
    Response response = RestUtil.get(
        MODELARTS_CN_NORTH_V1_ENDPOINT + loginInfo.getProjectId() + SEPARATOR + MODELARTS_MODEL
            + SEPARATOR + modelId, loginInfo.getToken(), client);
    if (response.isSuccessful()) {
      ObjectMapper objectMapper = new ObjectMapper();
      String rspStr = response.body().string();
      LOGGER.info(rspStr);
      Map<String, Object> jsonNodeMap =
          objectMapper.readValue(rspStr, new TypeReference<Map<String, Object>>() {
          });
      Map<String, String> modelDetail = new HashMap<>();
      modelDetail.put("modelName", jsonNodeMap.get("model_name").toString());
      modelDetail.put("modelType", jsonNodeMap.get("model_type").toString());
      modelDetail.put("modelSize", jsonNodeMap.get("model_size").toString());
      modelDetail.put("modelStatus", jsonNodeMap.get("model_status").toString());
      modelDetail.put("modelVersion", jsonNodeMap.get("model_version").toString());
      modelDetail.put("json", rspStr);
      return modelDetail;
    } else {
      throw new Exception("Imported model retrieval failed" + response.body().string());
    }
  }

}


