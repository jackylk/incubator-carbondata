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

package leo.qs.controller;

import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.runner.Router;

import fleet.core.StandardQueryRunner;
import leo.fleet.router.AsyncJob;
import leo.fleet.router.JobID;
import leo.fleet.router.Query;
import leo.fleet.router.QueryRunner;
import leo.qs.client.MetaStoreClient;
import leo.qs.model.validate.RequestValidator;
import leo.qs.model.view.SqlRequest;
import leo.qs.model.view.SqlResponse;
import leo.qs.util.CarbonException;
import org.apache.log4j.Logger;
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
public class Controller {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(Controller.class.getName());

  private SparkSession session = Main.getSession();
  private QueryRunner queryRunner = new StandardQueryRunner(session);
  private MetaStoreClient metaClient;

  @RequestMapping(value = "/sql", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(@RequestBody SqlRequest request) throws CarbonException {
    RequestValidator.validateSql(request);
    String originSql = request.getSqlStatement();
    LogicalPlan plan = session.sql(originSql).queryExecution().analyzed();
    Query query = Router.route(session, originSql, plan);
    switch (query.getType()) {
      case PKQuery:
        // for primary key query, execute it synchronously
        List<CarbonRow> result = queryRunner.doPKQuery(query);
        return createResponse(request, result);

      case NPKQuery:
        // generate a new JobID and save in metastore
        JobID jobID = JobID.newRandomID();
        metaClient.setJobStarted(jobID, query);

        // execute the query asynchronously,
        // check the job status and get the result later by jobID
        AsyncJob job = queryRunner.doAsyncJob(query, jobID);
        return createAsyncResponse(request, job);

      default:
        return createExceptionResponse(request, new UnsupportedOperationException());
    }
  }

  private ResponseEntity<SqlResponse> createResponse(SqlRequest request, List<CarbonRow> result) {
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
