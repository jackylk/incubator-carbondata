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

package leo.qs.app;

import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.runner.Router;

import leo.qs.client.MetaStoreClient;
import leo.qs.intf.AsyncJob;
import leo.qs.intf.JobID;
import leo.qs.intf.Query;
import leo.qs.intf.QueryRunner;
import leo.qs.model.validate.RequestValidator;
import leo.qs.model.view.SqlRequest;
import leo.qs.model.view.SqlResponse;
import leo.qs.util.CarbonException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class Controller {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(Controller.class.getName());

  private SparkSession session = Main.getSession();
  private MetaStoreClient metaClient;
  private RunnerLocator locator = new LocalRunnerLocator();

  @RequestMapping(value = "/sql", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(@RequestBody SqlRequest request) throws CarbonException {
    RequestValidator.validateSql(request);
    String originSql = request.getSqlStatement();
    LogicalPlan plan;
    try {
      // if it is a DDL, it will be executed here
      plan = session.sql(originSql).queryExecution().analyzed();
    } catch (Exception e) {
      return createExceptionResponse(request, e);
    }

    // decide whether the query is primary key query or not
    Query query = Router.route(session, originSql, plan);
    QueryRunner queryRunner = locator.getRunner(query);
    switch (query.getType()) {
      case PKQuery:
        // for primary key query, execute it synchronously
        List<Row> result = queryRunner.doPKQuery(query);
        return createResponse(request, result);

      case NPKQuery:
        // generate a new JobID and save in metastore
        JobID jobID1 = JobID.newRandomID();
        metaClient.setJobStarted(jobID1, query);

        // execute the query asynchronously,
        // check the job status and get the result later by jobID
        AsyncJob job1 = queryRunner.doAsyncJob(query, jobID1);
        return createAsyncResponse(request, job1);

      case ContinuousQuery:
        // generate a new JobID and save in metastore
        JobID jobID2 = JobID.newRandomID();
        metaClient.setJobStarted(jobID2, query);

        // execute the continuous query asynchronously,
        AsyncJob job2 = queryRunner.doContinuousJob(query, jobID2);
        return createAsyncResponse(request, job2);

      default:
        return createExceptionResponse(request, new UnsupportedOperationException());
    }
  }

  private ResponseEntity<SqlResponse> createResponse(SqlRequest request, List<Row> result) {
    return new ResponseEntity<>(
        new SqlResponse(request, "SUCCESS", null), HttpStatus.OK);
  }

  private ResponseEntity<SqlResponse> createAsyncResponse(SqlRequest request, AsyncJob job) {
    return new ResponseEntity<>(
        new SqlResponse(request, "SUCCESS", null), HttpStatus.OK);
  }

  private ResponseEntity<SqlResponse> createExceptionResponse(SqlRequest request, Exception e) {
    return new ResponseEntity<>(
        new SqlResponse(request, "FAILED", null), HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @RequestMapping(value = "/fetchSqlResult", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> fetchSqlResult(@RequestParam(name = "name") String name) {
    return null;
  }

  @RequestMapping(value = "/getSqlStatus", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getSqlStatus(@RequestParam(name = "name") String name) {
    return null;
  }

  @RequestMapping(value = "echosql")
  public ResponseEntity<String> echosql(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(String.valueOf("Welcome to SQL, " + name), HttpStatus.OK);
  }
}
