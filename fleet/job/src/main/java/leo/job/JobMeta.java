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

public class JobMeta {
  private JobID jobId;
  private long startTs;
  private long endTs;
  private String query;
  private int status;
  private String path;

  public JobMeta(JobID jobId, long startTs, long endTs, String query, int status, String path) {
    this.jobId = jobId;
    this.startTs = startTs;
    this.endTs = endTs;
    this.query = query;
    this.status = status;
    this.path = path;
  }

  public JobID getJobId() {
    return jobId;
  }

  public void setJobId(JobID jobId) {
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
}
