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

package org.apache.carbondata.leo.queryserver.runner;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.leo.job.query.AsyncJob;
import org.apache.carbondata.leo.job.query.JobMeta;
import org.apache.carbondata.leo.job.define.Query;
import org.apache.carbondata.leo.queryserver.model.view.SqlResult;

/**
 * Implement this to support query job on fleet compute cluster.
 */
public interface QueryRunner {

  /**
   * perform an asynchronous query job.
   *
   * @param query query request
   * @param jobMeta
   * @return job handler
   */
  AsyncJob doAsyncJob(Query query, JobMeta jobMeta);

  /**
   * perform a synchronous query job.
   *
   * @param query query request
   * @return query result
   */
  SqlResult doSyncJob(Query query);

  /**
   * get job meta from query runner cache or meta store client if not found.
   * @param  jobId
   * @return job JobMeta
   */
  JobMeta getJobMeta(String jobId, String projectId);

  /**
   * get query result page from file path according to offset and limit for async job by
   * local reader impl.
   * @param  path of query result
   * @return result
   */
  List<String[]> fetchResultPage(String path, int startLineNum, int limit)
      throws IOException;

}
