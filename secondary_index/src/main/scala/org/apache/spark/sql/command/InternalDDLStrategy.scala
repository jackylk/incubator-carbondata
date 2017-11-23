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

package org.apache.spark.sql.command

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.{DropIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.ExecutedCommandExec

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * carbon strategy for internal DDL command
 */
class InternalDDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CreateIndexTable(indexModel, tableProperties, createIndexSql, isCreateSIndex) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .tableExists(TableIdentifier(indexModel.tableName, indexModel.databaseName))(
            sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(CreateIndexTable(indexModel, tableProperties, createIndexSql,
            isCreateSIndex)) :: Nil
        } else {
          sys.error("Operation not allowed or Missing privileges : " + createIndexSql)
        }
      case showIndex@ShowIndexesCommand(databaseName, table, showIndexSql) =>
        try {
          // if table exist and permission is provided after launching spark-sql,
          // then below method will refresh carbonCatalog metadata for new ACL
          // Else throws table not found exception, which means not carbon table.
          CarbonEnv.getInstance(sparkSession).carbonMetastore
            .lookupRelation(TableIdentifier(table, databaseName))(sparkSession)
          ExecutedCommandExec(ShowIndexes(databaseName, table, showIndexSql, plan.output)) ::
          Nil
        } catch {
          case c: Exception =>
            sys.error("Operation not allowed or Missing privileges : " + showIndexSql)
        }
      case DropIndexCommand(ifExistsSet, databaseNameOp,
      tableName, parentTableName, dropIndexSql) =>
        val tableIdentifier = TableIdentifier(parentTableName, databaseNameOp)
        val isParentTableExists = sparkSession.sessionState.catalog.tableExists(tableIdentifier)
        if (!isParentTableExists) {
          if (ifExistsSet) {
            ExecutedCommandExec(CarbonDummyCommand()) :: Nil
          } else {
            sys.error("Table does not exist or Missing privileges : " + dropIndexSql)
          }
        } else {
          val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
            .tableExists(tableIdentifier)(sparkSession)
          if (isCarbonTable) {
            val isIndexTableExist = CarbonEnv.getInstance(sparkSession).carbonMetastore
              .tableExists(TableIdentifier(tableName, databaseNameOp))(sparkSession)
            if (!isIndexTableExist && !ifExistsSet) {
              val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
              throw new MalformedCarbonCommandException(
                s"Index table [$dbName.$tableName] does not exist on " +
                s"parent table [$dbName.$parentTableName]")
            }
            ExecutedCommandExec(DropIndex(ifExistsSet, databaseNameOp,
              tableName, parentTableName)) :: Nil
          } else {
            sys.error("Operation not allowed or Missing privileges : " + dropIndexSql)
          }
        }
      case RegisterIndexTableCommand(dbName, indexTableName, parentTable, registerSql) =>
        ExecutedCommandExec(RegisterIndexTableCommand(dbName, indexTableName,
          parentTable, registerSql)) :: Nil
      case _ => Nil
    }
  }
}
