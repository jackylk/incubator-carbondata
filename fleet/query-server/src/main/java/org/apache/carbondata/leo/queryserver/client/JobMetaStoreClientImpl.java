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

import org.apache.carbondata.leo.job.query.JobConf;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.carbondata.leo.job.query.AsyncJobStatus;
import org.apache.carbondata.leo.job.query.JobID;
import org.apache.carbondata.leo.job.query.JobMeta;
import org.apache.carbondata.leo.job.define.Query;
import org.apache.carbondata.leo.queryserver.util.CarbonResultUtil;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;

public class JobMetaStoreClientImpl implements JobMetaStoreClient {

  private String endPoint = "";
  private boolean mock;
  private final String GET_JOB_STATUS = "/v1/project_id/jobs/jobId";
  private final String CREATE_JOB_STATUS = "/v1/project_id/jobs/";
  private final String UPDATE_JOB_STATUS = "/v1/project_id/jobs/jobId";

  private static Logger LOGGER =
      LogServiceFactory.getLogService(JobMetaStoreClientImpl.class.getCanonicalName());
  @Autowired
  RestTemplateClient restTemplateClient;

  @Override
  public JobMeta getJobStatus(String jobID, String projectId) {
    try {
      String getUrl = GET_JOB_STATUS
          .replace("project_id", projectId)
          .replace("jobId", jobID);
      JobMeta jobMeta = (JobMeta)restTemplateClient
          .get(endPoint + getUrl, JobMeta.class);
      return jobMeta;
    } catch (Throwable e) {
      LOGGER.error(e.getMessage());
      throw e;
    }
  }

  @Override
  public JobMeta setJobStarted(JobID jobID, Query query, String projectId) {
    JobMeta jobMeta = new JobMeta();
    try {
      jobMeta.setJobId(jobID.getId());
      jobMeta.setStartTs(System.currentTimeMillis());
      jobMeta.setEndTs(System.currentTimeMillis());
      jobMeta.setQuery(query.getOriginSql());
      jobMeta.setStatus(AsyncJobStatus.STARTED.getStatus());
      jobMeta.setPath(query.getResultPath());
      String schema = CarbonResultUtil.convertSchemaToJson(query.getOriginPlan().schema());
      jobMeta.setJobSchema(schema);
      jobMeta.setClusterName(System.getProperty(JobConf.LEO_CLUSTER_NAME));
      jobMeta.setProjectId(projectId);
      HttpEntity<JobMeta> request = new HttpEntity<JobMeta>(jobMeta);
      String createUrl = CREATE_JOB_STATUS
          .replace("project_id", projectId)
          .replace("jobId", jobMeta.getJobId());
      restTemplateClient.post(endPoint + createUrl, request);
    } catch (Throwable e) {
      LOGGER.error(e.getMessage());
    }

    return jobMeta;
  }

  @Override
  public JobMeta setJobFinished(JobMeta jobMeta) {
    jobMeta.setEndTs(System.currentTimeMillis());
    jobMeta.setStatus(AsyncJobStatus.FINISHED.getStatus());
    HttpEntity<JobMeta> request = new HttpEntity<JobMeta>(jobMeta);
    String updateUrl = UPDATE_JOB_STATUS
        .replace("project_id", jobMeta.getProjectId())
        .replace("jobId", jobMeta.getJobId());
    restTemplateClient.put(endPoint + updateUrl, request);
    return jobMeta;
  }

  @Override
  public JobMeta setJobFailed(JobMeta jobMeta) {
    jobMeta.setEndTs(System.currentTimeMillis());
    jobMeta.setStatus(AsyncJobStatus.FAILED.getStatus());
    HttpEntity<JobMeta> request = new HttpEntity<JobMeta>(jobMeta);
    String updateUrl = UPDATE_JOB_STATUS.replace("project_id", jobMeta.getProjectId())
        .replace("jobId", jobMeta.getJobId());;
    restTemplateClient.put(endPoint + updateUrl, request);
    return jobMeta;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public boolean isMock() {
    return mock;
  }

  public void setMock(boolean mock) {
    this.mock = mock;
  }
}
