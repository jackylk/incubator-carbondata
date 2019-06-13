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

package leo.fleet.router;

public class Query {

  private String originSql;

  private String rewrittenSql;

  private KVQueryParams kvQueryParams;

  private Type type;

  public enum Type {
    PKQuery, NPKQuery
  }

  private Query(String originSql, Type type) {
    this.originSql = originSql;
    this.type = type;
  }

  /**
   * create a query containing primary key filter
   * @param originSql
   * @param kvQueryParams
   * @return a new Query object
   */
  public static Query makePKQuery(String originSql, KVQueryParams kvQueryParams) {
    Query query = new Query(originSql, Type.PKQuery);
    query.kvQueryParams = kvQueryParams;
    return query;
  }

  /**
   * create a query without primary key filter
   * @param originSql
   * @param rewrittenSql
   * @return a new Query object
   */
  public static Query makeNPKQuery(String originSql, String rewrittenSql) {
    Query query = new Query(originSql, Type.NPKQuery);
    query.rewrittenSql = rewrittenSql;
    return query;
  }

  public String getRewrittenSql() {
    return rewrittenSql;
  }

  public KVQueryParams getKvQueryParams() {
    return kvQueryParams;
  }

  public Type getType() {
    return type;
  }

  public String getOriginSql() {
    return originSql;
  }

}
