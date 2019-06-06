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

public class HBaseScanRequest {

  private String databaseName;
  private String tableName;
  private String[] selectColumns;
  private String filterExpr;
  private int limit;

  public HBaseScanRequest() {
  }

  public HBaseScanRequest(String databaseName, String tableName, String[] select, String filterExpr,
      int limit) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.selectColumns = select;
    this.filterExpr = filterExpr;
    this.limit = limit;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getSelect() {
    return selectColumns;
  }

  public void setSelect(String[] selectColumns) {
    this.selectColumns = selectColumns;
  }

  public String getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(String filterExpr) {
    this.filterExpr = filterExpr;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

}
