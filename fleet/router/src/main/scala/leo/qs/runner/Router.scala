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

package leo.qs.runner

import java.util

import leo.job.{Query, QueryDef}
import leo.job.QueryDef.QueryType
import org.apache.spark.sql.{
  CarbonDatasourceHadoopRelation, CarbonEnv, DeleteRecords,
  SparkSession, UpdateTable
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.datamap.{
  CarbonCreateDataMapCommand,
  CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand
}
import org.apache.spark.sql.execution.command.management.{
  CarbonAlterTableCompactionCommand,
  CarbonAlterTableFinishStreaming, CarbonCleanFilesCommand,
  CarbonDeleteLoadByIdCommand, CarbonDeleteLoadByLoadDateCommand, CarbonInsertColumnsCommand,
  CarbonLoadDataCommand, CarbonShowLoadsCommand
}
import org.apache.spark.sql.execution.command.mutation.{
  CarbonProjectForDeleteCommand,
  CarbonProjectForUpdateCommand
}
import org.apache.spark.sql.execution.command.partition.{
  CarbonAlterTableAddHivePartitionCommand,
  CarbonAlterTableDropHivePartitionCommand,
  CarbonAlterTableSplitPartitionCommand
}
import org.apache.spark.sql.execution.command.schema.{
  CarbonAlterTableAddColumnCommand,
  CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand,
  CarbonAlterTableRenameCommand, CarbonAlterTableSetCommand, CarbonAlterTableUnsetCommand
}
import org.apache.spark.sql.execution.command.{
  CreateDatabaseCommand, DescribeColumnCommand,
  DescribeTableCommand, DropDatabaseCommand, DropTableCommand, ExplainCommand, LoadDataCommand,
  RunnableCommand, SetCommand, SetDatabaseCommand, ShowDatabasesCommand, ShowTablesCommand
}
import org.apache.spark.sql.execution.command.stream.{
  CarbonCreateStreamCommand,
  CarbonDropStreamCommand, CarbonShowStreamsCommand
}
import org.apache.spark.sql.execution.command.table.CarbonExplainCommand
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.leo.command.{LeoCreateConsumerCommand, LeoCreateExperimentCommand,
  LeoCreateModelCommand, LeoDescConsumerCommand, LeoDropConsumerCommand,
  LeoDropExperimentCommand, LeoDropModelCommand, LeoRegisterModelCommand, LeoRunScriptCommand,
  LeoShowConsumersCommand, LeoShowModelsCommand, LeoUnregisterModelCommand}
import org.apache.spark.sql.util.SparkSQLUtil

object Router {

  def route(session: SparkSession, originSql: String, unsolvedPlan: LogicalPlan): Query = {
    if (isRunnableCmdOrPlan(unsolvedPlan)) {
      unsolvedPlan match {
        ///////////////////////////////////////////////////////////////
        //                            DML                            //
        ///////////////////////////////////////////////////////////////
        case cmd@CarbonLoadDataCommand(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.LOAD.name())
        case cmd@LoadDataCommand(_, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.LOAD.name())
        case insert@InsertIntoTable(_, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, insert, originSql,
            QueryType.CARBON_INSERT_SELECT.name())
        case update@UpdateTable(table, columns, selectStmt, alias, filer) =>
          Query.makeQueryWithTypeName(originSql, update, originSql, QueryType.BULK_UPDATE.name())
        case delete@DeleteRecords(statement, alias, table) =>
          Query.makeQueryWithTypeName(originSql, delete, originSql, QueryType.BULK_DELETE.name())
        case cmd@CarbonDeleteLoadByIdCommand(_, databaseNameOp, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DELETE_LOAD_BY_ID.name())
        case cmd@CarbonDeleteLoadByLoadDateCommand(databaseNameOp, _, _, _) =>
          Query
            .makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DELETE_LOAD_BY_DATE.name())
        case cmd@CarbonCleanFilesCommand(databaseNameOp, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CLEAN_FILES.name())
        case cmd@CarbonAlterTableCompactionCommand(model, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.COMPACTION.name())
        case cmd@CarbonProjectForDeleteCommand(_, databaseNameOp, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.BULK_DELETE.name())
        case cmd@CarbonProjectForUpdateCommand(_, databaseNameOp, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.BULK_UPDATE.name())

        ///////////////////////////////////////////////////////////////
        //                            DDL                            //
        ///////////////////////////////////////////////////////////////
        case cmd@CreateDatabaseCommand(databaseName, ifNotExists, path, comment, props) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_DATABASE.name())
        case cmd@DropDatabaseCommand(databaseName, ifExists, cascade) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_DATABASE.name())
        case cmd@CreateTable(table, saveMode, query) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_TABLE.name())
        case cmd@DropTableCommand(table, ifExists, isView, purge) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_TABLE.name())
        case cmd@SetDatabaseCommand(_) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SET_DATABASE.name())
        case cmd@ShowDatabasesCommand(databasePattern) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_DATABASES.name())
        case cmd@ShowTablesCommand(dbNameOp, t, isExtended, partitionSpec) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_TABLES.name())
        case cmd@DescribeTableCommand(table, partitionSpec, isExtended) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DESC_TABLE.name())
        case cmd@DescribeColumnCommand(table, colNameParts, isExtended) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DESC_COLUMN.name())
        case cmd@CarbonExplainCommand(_, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.EXPLAIN.name())
        case cmd@ExplainCommand(plan, extended, codegen, cost) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.EXPLAIN.name())
        case cmd@CarbonAlterTableAddColumnCommand(model) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_ADD_COLUMN.name())
        case cmd@CarbonAlterTableDropColumnCommand(model) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_DROP_COLUMN.name())
        case cmd@CarbonAlterTableAddHivePartitionCommand(table, _, _) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_ADD_PARTITION.name())
        case cmd@CarbonAlterTableDropHivePartitionCommand(table, _, _, _, _, _) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_DROP_PARTITION.name())
        case cmd@CarbonAlterTableColRenameDataTypeChangeCommand(model, _) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_RENAME_DATATYPE.name())
        case cmd@CarbonAlterTableRenameCommand(model) =>
          Query
            .makeQueryWithTypeName(originSql, cmd, originSql, QueryType.ALTER_TABLE_RENAME.name())
        case cmd@CarbonAlterTableSetCommand(tableIdentifier, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.ALTER_TABLE_SET.name())
        case cmd@CarbonAlterTableUnsetCommand(tableIdentifier, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.ALTER_TABLE_UNSET.name())
        case cmd@CarbonAlterTableSplitPartitionCommand(model) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.ALTER_TABLE_SPLIT_PARTITION.name())
        case cmd@CarbonShowLoadsCommand(databaseNameOp, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_LOADS.name())

        ///////////////////////////////////////////////////////////////
        //                          Consumer                         //
        ///////////////////////////////////////////////////////////////
        case cmd@LeoCreateConsumerCommand(_, _, _, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_CONSUMER.name())
        case cmd@LeoDropConsumerCommand(_, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_CONSUMER.name())
        case cmd@LeoDescConsumerCommand(_) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DESC_CONSUMER.name())
        case cmd@LeoShowConsumersCommand(_) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_CONSUMERS.name())

        ///////////////////////////////////////////////////////////////
        //                            Data Map                       //
        ///////////////////////////////////////////////////////////////
        case cmd@CarbonCreateDataMapCommand(_, tableOp, _, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_DATA_MAP.name())
        case cmd@CarbonDropDataMapCommand(_, _, tableOp, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_DATA_MAP.name())
        case cmd@CarbonDataMapShowCommand(tableOp) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_DATA_MAP.name())
        case cmd@CarbonDataMapRebuildCommand(_, tableOp) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.REBUILD_DATA_MAP.name())

        ///////////////////////////////////////////////////////////////
        //                            Stream                         //
        ///////////////////////////////////////////////////////////////
        case cmd@CarbonCreateStreamCommand(_, sinkDbNameOp, _, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_STREAM.name())
        case cmd@CarbonDropStreamCommand(_, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_STREAM.name())
        case cmd@CarbonShowStreamsCommand(tableOp) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_STREAM.name())
        case cmd@CarbonAlterTableFinishStreaming(dbNameOp, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.FINISH_STREAM.name())

        ///////////////////////////////////////////////////////////////
        //                          AI Query                         //
        ///////////////////////////////////////////////////////////////
        case cmd@CarbonInsertColumnsCommand(_, databaseNameOp, _, _) =>
          Query
            .makeQueryWithTypeName(originSql,
              cmd,
              originSql,
              QueryType.INSERT_COLUMNS.name())
        case cmd@LeoCreateModelCommand(_, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.CREATE_MODEL.name())
        case cmd@LeoDropModelCommand(_, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.DROP_MODEL.name())
        case cmd@LeoShowModelsCommand() =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SHOW_MODELS.name())
        case cmd@LeoCreateExperimentCommand(_, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.START_JOB.name())
        case cmd@LeoDropExperimentCommand(_, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.STOP_JOB.name())
        case cmd@LeoRegisterModelCommand(_, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.REGISTER_MODEL.name())
        case cmd@LeoUnregisterModelCommand(_, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.UNREGISTER_MODEL.name())
        case cmd@LeoRunScriptCommand(_, _, _, _) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.RUN_SCRIPT.name())

        ///////////////////////////////////////////////////////////////
        //                          Others                           //
        ///////////////////////////////////////////////////////////////
        case cmd@SetCommand(_) =>
          Query.makeQueryWithTypeName(originSql, cmd, originSql, QueryType.SET_COMMAND.name())

      }
    } else {
      val solvedPlan = SparkSQLUtil.ofRows(session, unsolvedPlan).queryExecution.analyzed
      solvedPlan match {
        // Other carbondata query goes here
        case queryPlan =>
          var tblProperties: util.Map[String, String] = null
          queryPlan.transform {
            case plan: LogicalPlan =>
              plan match {
                case alias@SubqueryAlias(_, _)
                  if alias.child.isInstanceOf[LogicalRelation] &&
                     alias.child.asInstanceOf[LogicalRelation].relation
                       .isInstanceOf[CarbonDatasourceHadoopRelation] =>
                  tblProperties = alias.child.asInstanceOf[LogicalRelation].relation
                    .asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.getTableInfo
                    .getFactTable.getTableProperties
                case others => others
              }
              plan
            case others => others
          }
          val rewrittenSql = rewriteCarbonQuery(originSql, unsolvedPlan)
          Query.makeNPKQuery(originSql, queryPlan, rewrittenSql, tblProperties)
      }
    }
  }

  def rewriteCarbonQuery(originSql: String, analyzed: LogicalPlan): String = {
    // TODO ->rewrite carbon sql string.
    originSql
  }

  // runnable cmd or some logical plan can not execute queryExecution to get solved plan, as they
  // will run once.
  def isRunnableCmdOrPlan(unsolvedPlan: LogicalPlan): Boolean = {
    unsolvedPlan.isInstanceOf[RunnableCommand] || unsolvedPlan.isInstanceOf[CreateTable] ||
    unsolvedPlan.isInstanceOf[InsertIntoTable] || unsolvedPlan.isInstanceOf[UpdateTable] ||
    unsolvedPlan.isInstanceOf[DeleteRecords]
  }
}