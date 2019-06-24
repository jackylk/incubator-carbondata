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
import java.util.List;

import org.apache.carbondata.core.datamap.status.DataMapStatus;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 * It updates the datamap status to the storage. It will have 2 implementations one will be disk
 * based and another would be DB based
 *
 * @version 1.4
 */
public interface TrainJobStorageProvider {

  /**
   * It reads and returns all enabled train job details from storage.
   *
   * @return TrainJobDetail[] all trainjob details
   */
  TrainJobDetail[] getAllTrainJobs(String modelName) throws IOException;


  /**
   * It reads and returns train job details from storage.
   *
   */
  TrainJobDetail getTrainJob(String modelName, String jobName) throws IOException;

  /**
   * Update the status of the given datamaps to the passed datamap status.
   *
   */
  void saveTrainJob(String modelName, TrainJobDetail jobDetail) throws IOException;

  /**
   * Drops the train job
   */
  void dropTrainJob(String modelName, String trainJobName) throws IOException;

  /**
   * Drops the model
   */
  void dropModel(String modelName) throws IOException;
}
