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

package leo.fleet.router;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;

/**
 * Implement this to support query job on fleet compute cluster.
 * It can be spark/presto/hive/hbasedirect
 */
@InterfaceAudience.Developer("fleet-engine")
public interface QueryRunner {

  /**
   * perform an asynchronous query job.
   * SQL statement maybe rewritten if there is any MV or Query Result Cache matched
   *
   * @param query query request
   */
  AsyncJob doAsyncJob(Query query, JobID jobID);

  /**
   * perform a synchronous query job.
   * SQL statement maybe rewritten if there is any MV or Query Result Cache matched
   *
   * @param query query request
   * @return query result
   */
  List<CarbonRow> doJob(Query query);

  /**
   * perform a synchronous query based on primary key.
   *
   * @param query query request
   * @return query result
   */
  List<CarbonRow> doPKQuery(Query query);
}
