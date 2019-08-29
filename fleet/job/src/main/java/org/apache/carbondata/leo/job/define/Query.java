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

package org.apache.carbondata.leo.job.define;

import java.util.Map;

import org.apache.carbondata.leo.job.define.QueryDef.QueryTypeDef;
import org.apache.carbondata.leo.job.query.JobConf;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import static org.apache.carbondata.leo.job.define.QueryDef.QueryType.CARBON_SELECT;

public class Query {

  private String originSql;

  private String rewrittenSql;

  private LogicalPlan originPlan;

  private Map<String, String> tblProperties;

  private String resultPath;

  private QueryTypeDef type;

  private KVQueryParams kvQueryParams;

  private Query(String originSql, QueryTypeDef type) {
    this.originSql = originSql;
    this.type = type;
  }

  /**
   * create a query containing primary key filter
   * @param originSql
   * @param kvQueryParams
   * @return a new Query object
   */

  public static Query makePKQuery(String originSql, LogicalPlan originPlan,
      KVQueryParams kvQueryParams) {
    Query query =
        new Query(originSql, QueryDef.getQueryTypeDef(QueryDef.QueryType.HBASE_SELECT.name()));
    query.originPlan = originPlan;
    query.kvQueryParams = kvQueryParams;
    return query;
  }

  /**
   * create a query without primary key filter
   * @param originSql
   * @param originPlan
   * @param rewrittenSql
   * @return a new Query object
   */
  public static Query makeNPKQuery(String originSql, LogicalPlan originPlan, String rewrittenSql,
      Map<String, String> tblProperties) {
    Query query = new Query(originSql, QueryDef.getQueryTypeDef(CARBON_SELECT.name()));
    query.originPlan = originPlan;
    query.rewrittenSql = rewrittenSql;
    query.tblProperties = tblProperties;
    return query;
  }

  /**
   * create a job with type name, dml ddl etc.
   * @param originSql
   * @param originPlan
   * @param rewrittenSql
   * @return a new Query object
   */
  public static Query makeQueryWithTypeName(String originSql, LogicalPlan originPlan,
      String rewrittenSql, String typeName) {
    Query query = new Query(originSql, QueryDef.getQueryTypeDef(typeName));
    query.originPlan = originPlan;
    query.rewrittenSql = rewrittenSql;
    return query;
  }

  /**
   * Generate the result path based on the query
   * @return result path
   */
  public String generateResultPathForQuery() {
    /*
     * Before create a leader, a bucket should be created with region name, the sql result would
     * store into a file like 'obs://${bucketName-this-region}/${dirName-this-cluster}/${jobId}',
     * Here, it only return the path, not include jobId, the jobId will set only after generated.
     */
    String bucketName = System.getProperty(JobConf.LEO_QUERY_BUCKET_NAME);
    String clusterName = System.getProperty(JobConf.LEO_CLUSTER_NAME);
    return "obs://" + bucketName + "/" + JobConf.QUERY_RESULT_DIR_PREFIX + clusterName + "/";
  }

  public String getRewrittenSql() {
    return rewrittenSql;
  }

  public KVQueryParams getKvQueryParams() {
    return kvQueryParams;
  }

  public QueryDef.QueryTypeDef getTypeDef() {
    return type;
  }

  public String getOriginSql() {
    return originSql;
  }

  public LogicalPlan getOriginPlan() {
    return originPlan;
  }

  public String getResultPath() {
    return resultPath;
  }

  public void setResultPath(String resultPath) {
    this.resultPath = resultPath;
  }

  public Map<String, String> getTblProperties() {
    return tblProperties;
  }

  public void setTblProperties(Map<String, String> tblProperties) {
    this.tblProperties = tblProperties;
  }
}
