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

package org.apache.carbondata.runner;

import org.apache.spark.sql.execution.command.RunnableCommand;

public class Query {

  private String rewrittenSql;

  private KVQueryParams KVQueryParams;

  private RunnableCommand ddlCommand;

  private Type type;

  public enum Type {
    DDL, PKQuery, NPKQuery
  }

  private Query(Type type) {
    this.type = type;
  }

  /**
   * create a query containing primary key filter
   * @param KVQueryParams
   * @return a new Query object
   */
  public static Query makePKQuery(KVQueryParams KVQueryParams) {
    Query query = new Query(Type.PKQuery);
    query.KVQueryParams = KVQueryParams;
    return query;
  }

  /**
   * create a query without primary key filter
   * @param rewrittenSql
   * @return a new Query object
   */
  public static Query makeNPKQuery(String rewrittenSql) {
    Query query = new Query(Type.NPKQuery);
    query.rewrittenSql = rewrittenSql;
    return query;
  }

  /**
   * create a DDL query
   * @param ddlCommand
   * @return a new Query object
   */
  public static Query makeDDLQuery(RunnableCommand ddlCommand) {
    Query query = new Query(Type.DDL);
    query.ddlCommand = ddlCommand;
    return query;
  }

  public String getRewrittenSql() {
    return rewrittenSql;
  }

  public KVQueryParams getPrimaryKeyFilterQuery() {
    return KVQueryParams;
  }

  public org.apache.carbondata.runner.KVQueryParams getKVQueryParams() {
    return KVQueryParams;
  }

  public RunnableCommand getDdlCommand() {
    return ddlCommand;
  }

  public Type getType() {
    return type;
  }

}
