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

package org.apache.carbondata.compute.controller;

import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.compute.model.validate.RequestValidator;
import org.apache.carbondata.compute.model.view.SqlRequest;
import org.apache.carbondata.compute.model.view.SqlResponse;
import org.apache.carbondata.compute.util.CarbonException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.runner.AsyncJob;
import org.apache.carbondata.runner.DDLRunner;
import org.apache.carbondata.runner.Query;
import org.apache.carbondata.runner.QueryRunner;
import org.apache.carbondata.runner.Router;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
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

  private QueryRunner queryRunner;
  private DDLRunner ddlRunner;

  @RequestMapping(value = "/sql", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(@RequestBody SqlRequest request) throws CarbonException {
    RequestValidator.validateSql(request);
    List<CarbonRow> result;
    SparkSession session = Main.getSession();
    Query query = Router.route(session, request.getSqlStatement());
    switch (query.getType()) {
      case DDL:
        result = ddlRunner.doDDL(query);
        return createResponse(request, result);
      case PKQuery:
        result = queryRunner.doPKQuery(query.getKVQueryParams());
        return createResponse(request, result);
      case NPKQuery:
        AsyncJob job = queryRunner.doAsyncJob(query.getRewrittenSql());
        return createAsyncResponse(request, job);
      default:

        // TODO: create failure response
        throw new UnsupportedOperationException();
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
