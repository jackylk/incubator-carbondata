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

package org.apache.carbondata.router

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.router.RewrittenQuery.QueryType

object Router {

  def route(session: SparkSession, sqlString: String): RewrittenQuery = {
    val analyzed = session.sql(sqlString).queryExecution.analyzed
    analyzed match {
      case _@Project(columns, _@Filter(expr, s: SubqueryAlias))
        if containsPrimaryKey(expr) &&
           s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val dest = new RewrittenQuery(QueryType.HBASE, analyzed)
        val scan = new HBaseScanRequest()
        //TODO: set param in scan
        dest.setHbaseScanRequest(scan)
        dest
      case gl@GlobalLimit(_, ll@LocalLimit(_, p@Project(columns, _@Filter(expr, s: SubqueryAlias))))
        if containsPrimaryKey(expr) &&
           s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val dest = new RewrittenQuery(QueryType.HBASE, analyzed)
        val scan = new HBaseScanRequest()
        //TODO: set param in scan
        dest.setHbaseScanRequest(scan)
        dest
      case _ =>
        val dest = new RewrittenQuery(QueryType.CARBON, analyzed)
        val rewrittenSql = rewriteCarbonQuery(analyzed)
        dest.setRewrittenSql(rewrittenSql)
        dest
    }
  }

  private def containsPrimaryKey(expr: Expression): Boolean = ???

  private def rewriteCarbonQuery(analyzed: LogicalPlan): String = ???
}
