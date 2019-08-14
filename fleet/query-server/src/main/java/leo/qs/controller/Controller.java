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


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import leo.job.AsyncJobStatus;
import leo.job.JobMeta;
import leo.job.QueryDef;
import leo.model.view.Column;
import leo.model.view.Schema;
import leo.model.view.SqlResult;
import leo.qs.Main;
import leo.qs.exception.ErrorCode;
import leo.qs.exception.JobStatusException;
import leo.qs.exception.LeoServiceException;

import leo.qs.locator.RunnerLocator;
import leo.qs.model.view.FetchSqlResultResponse;
import leo.qs.model.view.GetSqlStatusResponse;
import leo.qs.model.view.SqlRequest;
import leo.qs.model.view.SqlResponse;
import leo.qs.runner.Router;

import org.apache.carbondata.common.logging.LogServiceFactory;

import leo.job.AsyncJob;
import leo.job.JobID;
import leo.job.JobMetaStoreClient;
import leo.job.Query;
import leo.qs.intf.QueryRunner;
import leo.qs.model.validate.RequestValidator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

  private static final Logger LOGGER = LogServiceFactory.getLogService(Controller.class.getName());

  @Autowired
  private JobMetaStoreClient metaClient;

  @Autowired
  private RunnerLocator locator;

  @RequestMapping(value = "/v1/{project_id}/sqls", method = RequestMethod.POST,
      consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SqlResponse> sql(
      @RequestBody SqlRequest request,
      @PathVariable(name = "project_id") String projectId) throws LeoServiceException {
    //Note: all LeoServiceException will handle by @ControllerAdvice together.
    RequestValidator.validateSql(request);
    String originSql = request.getSqlStatement();
    SparkSession session = Main.getSession();
    LogicalPlan unsolvedPlan;
    try {
      ParserInterface parser = session.sessionState().sqlParser();
      unsolvedPlan = parser.parsePlan(originSql);
    } catch (Exception e) {
      LOGGER.error("Failed to parse sql:::", e);
      if (e instanceof AnalysisException) {
        // analysisException should throw directly.
        throw new LeoServiceException(ErrorCode.LEO_ANALYSIS_ERROR.getCode(), e.getMessage());
      } else {
        throw new LeoServiceException(HttpStatus.INTERNAL_SERVER_ERROR.value(),
            HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
      }

    }
    // decide whether the type
    Query query = Router.route(session, originSql, unsolvedPlan);
    Schema schema = null;
    JobID jobID = JobID.newRandomID();;
    if (query.getOriginPlan().schema() != null) {
      schema = convertSchemaFromStructType(query);
    }
    QueryRunner queryRunner = locator.getRunner(query);
    // unsupported operations
    if (query.getTypeDef() == null || query.getTypeDef().getType() == null ||
        QueryDef.getQueryTypeDef(query.getTypeDef().getType().name()) == null) {
      throw new UnsupportedOperationException();
    }

    // async or not
    if (query.getTypeDef().isExecuteAsync()) {
      // generate a new JobID and save in metastore
      // obs path should re-write by jobId.
      query.setResultPath(query.generateResultPathForQuery() + jobID.getId());
      JobMeta jobMeta = metaClient.setJobStarted(jobID, query, projectId);
      // execute the query asynchronously,
      // check the job status and get the result later by jobID
      AsyncJob asyncJob = queryRunner.doAsyncJob(query, jobMeta);
      return createAsyncSqlResponse(query.getTypeDef().getType().name(), request,
          asyncJob.getJobId(), schema, null);
    } else {
        // others including ddl should execute sync
        try {
          SqlResult jobResult = queryRunner.doSyncJob(query);
          return createSyncSqlResponse(query.getTypeDef().getType().name(), request,
              jobID.getId(), jobResult.getSchema(), jobResult.getRows(),
              false);
        } catch (IllegalArgumentException ex) {
          throw new LeoServiceException(ErrorCode.LEO_ANALYSIS_ERROR.getCode(), ex.getMessage());
        } catch (Exception ex) {
          throw new LeoServiceException(ErrorCode.INTERNAL_ERROR.getCode(), ex.getMessage());
        }
    }
  }

  private static Schema convertSchemaFromStructType(Query query) throws LeoServiceException {
    StructField[] fields = query.getOriginPlan().schema().fields();
    boolean isHBaseQuery = query.getTypeDef().getType().equals(QueryDef.QueryType.HBASE_SELECT);
    Schema schema = new Schema();
    List<Column> columns = new ArrayList<>();
    Set<String> distinctNameSchema = new HashSet<String>();
    for (StructField field: fields) {
      Column column = new Column();
      column.setDataType(field.dataType().typeName());
      column.setName(field.name());
      column.setNullable(field.nullable());
      columns.add(column);
      distinctNameSchema.add(column.getName());
    }
    if (columns.size() != distinctNameSchema.size()) {
      throw new LeoServiceException(ErrorCode.LEO_ANALYSIS_ERROR.getCode(), "Same filed name"
          + " found in query project, leo does not support, please use as another name.");
    }
    schema.setColumns(columns);
    return schema;
  }

  private ResponseEntity<SqlResponse> createSyncSqlResponse(String type, SqlRequest request,
      String jobId, Schema schema, List<String[]> rows, boolean rowsMore) {
    SqlResponse sqlResponse = new SqlResponse(request, "SUCCESS", jobId, schema, rows);
    sqlResponse.setType(type);
    sqlResponse.setRowsMore(rowsMore);
    sqlResponse.setAsync(false);
    return new ResponseEntity<>(sqlResponse, HttpStatus.OK);
  }

  private ResponseEntity<SqlResponse> createSyncSqlResponse(String type, SqlRequest request,
                                                            String jobId, int rowCount) {
    SqlResponse sqlResponse = new SqlResponse(request, "SUCCESS", jobId, null, null);
    sqlResponse.setType(type);
    sqlResponse.setRowCount(rowCount);
    sqlResponse.setAsync(false);
    return new ResponseEntity<>(sqlResponse, HttpStatus.OK);
  }

  private ResponseEntity<SqlResponse> createAsyncSqlResponse(String type, SqlRequest request,
      String jobId, Schema schema, List<String[]> rows) {
    SqlResponse sqlResponse = new SqlResponse(request, "SUCCESS", jobId, schema, rows);
    sqlResponse.setType(type);
    sqlResponse.setRowsMore(false);
    sqlResponse.setAsync(true);
    return new ResponseEntity<>(sqlResponse, HttpStatus.OK);
  }

  @RequestMapping(value = "/v1/{project_id}/sqls/{jobId}/result",
                  produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<FetchSqlResultResponse> fetchSqlResult(
      @PathVariable(name = "project_id") String projectId,
      @PathVariable(name = "jobId") String jobId,
      @RequestParam(name = "offset") int offset,
      @RequestParam(name = "limit", required = false, defaultValue = "1000") int limit)
      throws Exception {
    RequestValidator.validateOffsetLimit(offset, limit);
    // init response
    FetchSqlResultResponse response =  new FetchSqlResultResponse(new SqlRequest(),
        "FAILED", jobId, null, null, false);
    try {
      QueryRunner queryRunner = locator.getRunner(null);
      JobMeta jobMeta = queryRunner.getJobMeta(jobId, projectId);
      if (jobMeta != null) {
        if (jobMeta.getStatus() == AsyncJobStatus.FINISHED.getStatus()) {
          List<String[]> dataList = queryRunner.fetchResultPage(jobMeta.getPath(), offset, limit);
          //set response
          response.setRows(dataList);
          response.setMessage("SUCCESS");
          if (dataList.size() < limit) {
            response.setRowsMore(false);
          } else {
            response.setRowsMore(true);
          }
        } else if (jobMeta.getStatus() == AsyncJobStatus.FAILED.getStatus()) {
          throw new JobStatusException(ErrorCode.JOB_FAILED_ERROR);
        } else if (jobMeta.getStatus() == AsyncJobStatus.STARTED.getStatus()) {
          throw new JobStatusException(ErrorCode.JOB_STILL_RUNNING_ERROR);
        }
      } else {
        throw new JobStatusException(ErrorCode.JOB_NOT_FOUND_ERROR);
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw new JobStatusException(ErrorCode.JOB_NO_RESULT_SHOW_ERROR);
      } else if (e instanceof JobStatusException) {
        throw e;
      } else {
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
      }
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @RequestMapping(value = "/v1/{project_id}/sqls/{jobId}/status",
                  produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<GetSqlStatusResponse> getSqlStatus(
      @PathVariable(name = "project_id") String projectId,
      @PathVariable(name = "jobId") String jobId) throws Exception {
    GetSqlStatusResponse response =  new GetSqlStatusResponse(new SqlRequest(), "Success",
        -1);
    QueryRunner queryRunner = locator.getRunner(null);
    JobMeta jobMeta = queryRunner.getJobMeta(jobId, projectId);
    if (jobMeta != null) {
      response.setStatus(jobMeta.getStatus());
      if (jobMeta.getStatus() == 3 && jobMeta.getFailedReason() != null) {
        response.setFailedReason(jobMeta.getFailedReason());
      }
    } else {
      throw new JobStatusException(ErrorCode.JOB_NOT_FOUND_ERROR);
    }
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @RequestMapping(value = "/echosql")
  public ResponseEntity<String> echosql(@RequestParam(name = "name") String name) {
    return new ResponseEntity<>(("Welcome to Leo SQL, " + name), HttpStatus.OK);
  }

}
