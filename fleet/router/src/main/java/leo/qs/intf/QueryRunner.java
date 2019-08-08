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

package leo.qs.intf;

import java.io.IOException;
import java.util.List;

import leo.job.AsyncJob;
import leo.job.JobMeta;
import leo.job.Query;
import leo.model.view.SqlResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Implement this to support query job on fleet compute cluster.
 * It can be spark/presto/hive/hbasedirect
 */
public interface QueryRunner {

  /**
   * perform an asynchronous query job.
   * SQL statement maybe rewritten if there is any MV or Query Result Cache matched
   *
   * @param query query request
   * @param jobMeta
   * @return job handler
   */
  AsyncJob doAsyncJob(Query query, JobMeta jobMeta);

  /**
   * perform a synchronous query job.
   * SQL statement maybe rewritten if there is any MV or Query Result Cache matched
   *
   * @param query query request
   * @return query result
   */
  SqlResult doJob(Query query);

  /**
   * get job meta from query runner cache or meta store client if not found.
   * @param  jobId
   * @return job JobMeta
   */
  JobMeta getJobMeta(String jobId, String projectId);

  /**
   * get all query result from file path for async job by spark dataframe.
   * @param  path of query result
   * @return result
   */
  Dataset<Row> fetchResult(String path);

  /**
   * get query result page from file path according to offset and limit for async job by
   * local reader impl.
   * @param  path of query result
   * @return result
   */
  List<String[]> fetchResultPage(String path, int startLineNum, int limit)
      throws Exception;

}
