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

package org.apache.carbondata.router;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class RewrittenQuery {

  private QueryType queryType;

  public enum QueryType {HBASE, CARBON}

  private LogicalPlan analyzed;

  private String rewrittenSql;

  private HBaseScanRequest hbaseScanRequest;

  public RewrittenQuery(QueryType queryType, LogicalPlan analyzed) {
    this.queryType = queryType;
    this.analyzed = analyzed;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public LogicalPlan getAnalyzed() {
    return analyzed;
  }

  public void setAnalyzed(LogicalPlan analyzed) {
    this.analyzed = analyzed;
  }

  public String getRewrittenSql() {
    return rewrittenSql;
  }

  public void setRewrittenSql(String rewrittenSql) {
    this.rewrittenSql = rewrittenSql;
  }

  public HBaseScanRequest getHbaseScanRequest() {
    return hbaseScanRequest;
  }

  public void setHbaseScanRequest(HBaseScanRequest hbaseScanRequest) {
    this.hbaseScanRequest = hbaseScanRequest;
  }
}
