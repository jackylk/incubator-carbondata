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
  public static TrainJobDetail[] getAllEnabledTrainedJobs(String experimentName)
      throws IOException {
    TrainJobDetail[] trainJobDetails = storageProvider.getAllTrainJobs(experimentName);
    List<TrainJobDetail> statusDetailList = new ArrayList<>();
    for (TrainJobDetail statusDetail : trainJobDetails) {
      if (statusDetail.getStatus() == TrainJobDetail.Status.CREATED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new TrainJobDetail[statusDetailList.size()]);
  }

  public static TrainJobDetail[] getAllTrainedJobs(String experimentName) throws IOException {
    return storageProvider.getAllTrainJobs(experimentName);
  }

  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static TrainJobDetail getTrainJob(
      String experimentName, String modelName) throws IOException {
    return storageProvider.getTrainJob(experimentName, modelName);
  }

  public static void dropTrainJob(String experimentName, String modelName) throws IOException {
    storageProvider.dropTrainJob(experimentName, modelName);
  }

  public static void dropModel(String experimentName) throws IOException {
    storageProvider.dropModel(experimentName);
  }

  public static void saveTrainJob(String experimentName, TrainJobDetail jobDetail)
      throws IOException {
    storageProvider.saveTrainJob(experimentName, jobDetail);
  }

}
