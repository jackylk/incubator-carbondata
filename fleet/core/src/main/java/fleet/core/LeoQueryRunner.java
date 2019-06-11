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
import org.apache.carbondata.runner.AsyncJob;
import org.apache.carbondata.runner.KVQueryParams;
import org.apache.carbondata.runner.QueryRunner;

/**
 * Do query on CarbonData by Spark
 */
public class LeoQueryRunner implements QueryRunner {

  @Override
  public AsyncJob doAsyncJob(String sqlString) {
    return null;
  }

  @Override
  public List<CarbonRow> doJob(String sqlString) {
    return null;
  }

  @Override
  public List<CarbonRow> doPKQuery(KVQueryParams params) {
    return null;
  }
}
