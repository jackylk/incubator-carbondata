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

package leo.job;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect
public class JobMeta {
  @JsonProperty("job_id")
  private String jobId;
  @JsonProperty("start_ts")
  private long startTs;
  @JsonProperty("end_ts")
  private long endTs;
  private String query;
  private int status;
  private String path;
  @JsonProperty("job_schema")
  private String jobSchema;
  @JsonProperty("cluster_name")
  private String clusterName;
  @JsonProperty("project_id")
  private String projectId;
  @JsonProperty("failed_reason")
  private String failedReason;

  public JobMeta() {
  }

  public JobMeta(String jobId, String projectId) {
    this.jobId = jobId;
    this.projectId = projectId;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public long getStartTs() {
    return startTs;
  }

  public void setStartTs(long startTs) {
    this.startTs = startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public void setEndTs(long endTs) {
    this.endTs = endTs;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getJobSchema() {
    return jobSchema;
  }

  public void setJobSchema(String jobSchema) {
    this.jobSchema = jobSchema;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getFailedReason() {
    return failedReason;
  }

  public void setFailedReason(String failedReason) {
    this.failedReason = failedReason;
  }
}
