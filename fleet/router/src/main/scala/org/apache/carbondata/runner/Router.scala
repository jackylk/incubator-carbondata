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

package org.apache.carbondata.runner

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{CreateDatabaseCommand, DDLUtils, DropDatabaseCommand, DropTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.command.{LeoCreateDatabaseCommand, LeoCreateTableCommand, LeoDropDatabaseCommand, LeoDropTableCommand}

object Router {

  def route(session: SparkSession, sqlString: String): Query = {
    val analyzed = session.sql(sqlString).queryExecution.analyzed
    analyzed match {

      // DROP DATABASE
      case cmd@CreateDatabaseCommand(_, _, _, _, _) =>
        Query.makeDDLQuery(LeoCreateDatabaseCommand(cmd))

      // DROP DATABASE
      case cmd@DropDatabaseCommand(_, _, _) =>
        Query.makeDDLQuery(LeoDropDatabaseCommand(cmd))

      // CREATE TABLE
      case cmd@org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, None)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        Query.makeDDLQuery(LeoCreateTableCommand(cmd, tableDesc, mode))

      // DROP TABLE
      case cmd@DropTableCommand(identifier, ifNotExists, _, _)
        if CarbonEnv.getInstance(session).carbonMetaStore.isTablePathExists(identifier)(session) =>
        Query.makeDDLQuery(
          LeoDropTableCommand(cmd, ifNotExists, identifier.database, identifier.table.toLowerCase)
        )

      // HBase query, in form of "SELECT column_list FROM t WHERE cond(primary_key)"
      case _@Project(columns, _@Filter(expr, s: SubqueryAlias))
        if containsPrimaryKey(expr) &&
           s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation =
          s.child.asInstanceOf[LogicalRelation].asInstanceOf[CarbonDatasourceHadoopRelation]
        Query.makePKQuery(
          new KVQueryParams(
            relation.carbonRelation.databaseName,
            relation.carbonRelation.tableName,
            columns.map(_.name).toArray,
            expr)
        )

      // HBase query, in form of "SELECT column_list FROM t WHERE cond(primary_key) LIMIT y
      case gl@GlobalLimit(_, ll@LocalLimit(_, p@Project(columns, _@Filter(expr, s: SubqueryAlias))))
        if containsPrimaryKey(expr) &&
           s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation =
          s.child.asInstanceOf[LogicalRelation].asInstanceOf[CarbonDatasourceHadoopRelation]
        Query.makePKQuery(
          new KVQueryParams(
            relation.carbonRelation.databaseName,
            relation.carbonRelation.tableName,
            columns.map(_.name).toArray,
            expr,
            gl.maxRows.get)
        )

      // Other carbondata query goes here
      case _ =>
        val rewrittenSql = rewriteCarbonQuery(analyzed)
        Query.makeNPKQuery(rewrittenSql)
    }
  }

  private def containsPrimaryKey(expr: Expression): Boolean = {
    false
  }

  private def rewriteCarbonQuery(analyzed: LogicalPlan): String = {
    null
  }
}
