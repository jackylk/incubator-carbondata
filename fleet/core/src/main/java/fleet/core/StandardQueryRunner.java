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

package fleet.core;

import java.util.List;

import org.apache.carbondata.core.datastore.row.CarbonRow;

import leo.fleet.router.AsyncJob;
import leo.fleet.router.JobID;
import leo.fleet.router.Query;
import leo.fleet.router.QueryRunner;
import org.apache.spark.sql.SparkSession;

/**
 * Do PKQuery by constructing scan query to hbase locally,
 * otherwise execute the job by spark.
 */
public class StandardQueryRunner implements QueryRunner {

  private SparkSession session;

  public StandardQueryRunner(SparkSession session) {
    this.session = session;
  }

  @Override
  public AsyncJob doAsyncJob(Query query, JobID jobID) {
    return null;
  }

  @Override
  public List<CarbonRow> doJob(Query query) {
    return null;
  }

  @Override
  public List<CarbonRow> doPKQuery(Query query) {
    return null;
  }
}
