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
package org.apache.leo.model.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * Maintains the status of each datamap. As per the status query will decide whether to hit
 * datamap or not.
 */
public class TrainJobManager {

  // Create private constructor to not allow create instance of it
  private TrainJobManager() {

  }

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(
          TrainJobManager.class.getName());

  /**
   * TODO Use factory when we have more storage providers
   */
  private static TrainJobStorageProvider storageProvider =
      new DiskBasedTrainJobProvider(CarbonProperties.getInstance().getSystemFolderLocation()
          + CarbonCommonConstants.FILE_SEPARATOR + "model");


  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static TrainJobDetail[] getAllEnabledTrainedJobs(String modelName) throws IOException {
    TrainJobDetail[] trainJobDetails = storageProvider.getAllTrainJobs(modelName);
    List<TrainJobDetail> statusDetailList = new ArrayList<>();
    for (TrainJobDetail statusDetail : trainJobDetails) {
      if (statusDetail.getStatus() == TrainJobDetail.Status.CREATED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new TrainJobDetail[statusDetailList.size()]);
  }

  public static TrainJobDetail[] getAllTrainedJobs(String modelName) throws IOException {
    return storageProvider.getAllTrainJobs(modelName);
  }

  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static TrainJobDetail getTrainJob(
      String modelName, String trainJobName) throws IOException {
    return storageProvider.getTrainJob(modelName, trainJobName);
  }

  public static void dropTrainJob(String modelName, String jobName) throws IOException {
    storageProvider.dropTrainJob(modelName, jobName);
  }

  public static void dropModel(String modelName) throws IOException {
    storageProvider.dropModel(modelName);
  }

  public static void saveTrainJob(String modelName, TrainJobDetail jobDetail) throws IOException {
    storageProvider.saveTrainJob(modelName, jobDetail);
  }

}
