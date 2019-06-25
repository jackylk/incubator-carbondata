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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.cache.{CarbonDropCacheCommand, CarbonShowCacheCommand}
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonAlterTableFinishStreaming, CarbonCleanFilesCommand, CarbonCliCommand, CarbonDeleteLoadByIdCommand, CarbonDeleteLoadByLoadDateCommand, CarbonInsertColumnsCommand, CarbonInsertIntoCommand, CarbonShowLoadsCommand, RefreshCarbonTableCommand}
import org.apache.spark.sql.execution.command.mutation.{CarbonProjectForDeleteCommand, CarbonProjectForUpdateCommand}
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand, CarbonAlterTableDropPartitionCommand, CarbonAlterTableSplitPartitionCommand, CarbonShowCarbonPartitionsCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand, CarbonAlterTableRenameCommand, CarbonAlterTableSetCommand, CarbonAlterTableUnsetCommand}
import org.apache.spark.sql.execution.command.stream.{CarbonCreateStreamCommand, CarbonDropStreamCommand, CarbonShowStreamsCommand}
import org.apache.spark.sql.execution.datasources.CreateTable

object LeoDatabase {
  var DEFAULT_PROJECTID: String = "DEFAULTTENANT666"

  /**
   * convert user table identifier to leo table identifier
   */
  def convertUserTableIdentifierToLeo(userTable: TableIdentifier): TableIdentifier = {
    TableIdentifier(
      userTable.table,
      Some(convertUserDBNameToLeo(userTable.database.get)))
  }

  /**
   * convert user database name to leo database name
   */
  def convertUserDBNameToLeo(userDBName: String): String = {
    leoDBNamePrefix + userDBName
  }

  /**
   * convert leo database name to user database name
   */
  def convertLeoDBNameToUser(leoDBName: String): String = {
    leoDBName.substring(leoDBNamePrefix.length)
  }

  // TODO: add user projectid here
  var leoDBNamePrefix: String = DEFAULT_PROJECTID

  /**
   * Return false if plan is invalid, otherwise return updated plan after
   * modifying database name that user gives.
   *
   * Following case the plan is invalid:
   * 1. user does not give database name
   * 2. USE DATABASE command
   */
  def convertUserDBNameToLeoInPlan(parsedPlan: LogicalPlan): (Option[LogicalPlan], String) = {
    // In order to support same DB name for different tenant,
    // we do following checks and modification of the DB name
    // 1. For all commands, ensure database name must exist and not default,
    // otherwise return false
    // 2. USE DATABASE command is not allowed
    // 3. For all commands, add ProjectID prefix in database name after parser
    // 4. For command that expose database name, wrap it by Leo command and
    // remove the ProjectID before return to user

    val updatedPlan = parsedPlan.transform {
      case relation: UnresolvedRelation =>
        if (relation.tableIdentifier.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (relation.tableIdentifier.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        } else {
          UnresolvedRelation(LeoDatabase.convertUserTableIdentifierToLeo(relation.tableIdentifier))
        }

      case cmd@CreateDatabaseCommand(databaseName, ifNotExists, path, comment, props) =>
        if (databaseName.equals("default")) {
          return (None, "database name default is not allowed")
        }
        val db = LeoDatabase.convertUserDBNameToLeo(databaseName)
        CreateDatabaseCommand(db, ifNotExists, path, comment, props)

      case cmd@DropDatabaseCommand(databaseName, ifExists, cascade) =>
        val db = LeoDatabase.convertUserDBNameToLeo(databaseName)
        DropDatabaseCommand(db, ifExists, cascade)

      case cmd@CreateTable(table, saveMode, query) =>
        requireDBNameNonEmpty(table.identifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          table.identifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.identifier.database.get)))
        CreateTable(table.copy(identifier = newTable), saveMode, query)

      case cmd@DropTableCommand(table, ifExists, isView, purge) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        DropTableCommand(newTable, ifExists, isView, purge)

      case cmd@SetDatabaseCommand(_) =>
        return (None, "use database command is not allowed, " +
                      "specify database name in the query instead")

      case cmd@ShowDatabasesCommand(databasePattern) =>
        if (databasePattern.isDefined) {
          return (None, "database pattern is not supported")
        }
        ShowDatabasesCommand(Some(LeoDatabase.leoDBNamePrefix + "*"))

      case cmd@ShowTablesCommand(dbNameOp, t, isExtended, partitionSpec) =>
        requireDBNameNonEmpty(dbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        ShowTablesCommand(
          Some(LeoDatabase.convertUserDBNameToLeo(dbNameOp.get)), t, isExtended, partitionSpec)

      case cmd@DescribeTableCommand(table, partitionSpec, isExtended) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        DescribeTableCommand(
          LeoDatabase.convertUserTableIdentifierToLeo(table), partitionSpec, isExtended)

      case cmd@ExplainCommand(plan, extended, codegen, cost) =>
        val (newPlanOp, msg) = convertUserDBNameToLeoInPlan(plan)
        if (newPlanOp.isEmpty) {
          return (None, msg)
        }
        ExplainCommand(newPlanOp.get, extended, codegen, cost)

        //////////////////////////////////////////////////
        //                Alter Table                   //
        //////////////////////////////////////////////////

      case cmd@CarbonAlterTableAddColumnCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableAddColumnsModel = newModel)

      case cmd@CarbonAlterTableDropColumnCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableDropColumnModel = newModel)

      case cmd@CarbonAlterTableColRenameDataTypeChangeCommand(model, _) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableColRenameAndDataTypeChangeModel = newModel)

      case cmd@CarbonAlterTableRenameCommand(model) =>
        requireDBNameNonEmpty(model.oldTableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        requireDBNameNonEmpty(model.newTableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val src = model.oldTableIdentifier.copy(
          database = Some(LeoDatabase.convertUserDBNameToLeo(
            model.oldTableIdentifier.database.get)))
        val dest = model.newTableIdentifier.copy(
          database = Some(LeoDatabase.convertUserDBNameToLeo(
            model.newTableIdentifier.database.get)))
        val newModel = model.copy(oldTableIdentifier = src, newTableIdentifier = dest)
        cmd.copy(alterTableRenameModel = newModel)

      case cmd@CarbonAlterTableSetCommand(tableIdentifier, _, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(
          tableIdentifier = new TableIdentifier(
            tableIdentifier.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonAlterTableUnsetCommand(tableIdentifier, _, _, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(
          tableIdentifier = new TableIdentifier(
            tableIdentifier.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonAlterTableCompactionCommand(model, _, _) =>
        requireDBNameNonEmpty(model.dbName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          dbName = Some(LeoDatabase.convertUserDBNameToLeo(model.dbName.get)))
        cmd.copy(alterTableModel = newModel)

      case cmd@CarbonAlterTableFinishStreaming(dbNameOp, _) =>
        requireDBNameNonEmpty(dbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(dbName = Some(LeoDatabase.convertUserDBNameToLeo(dbNameOp.get)))


      //////////////////////////////////////////////////
      //                Partition                     //
      //////////////////////////////////////////////////

      case cmd@CarbonAlterTableAddHivePartitionCommand(table, _, _) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        cmd.copy(tableName = newTable)

      case cmd@CarbonAlterTableDropHivePartitionCommand(table, _, _, _, _, _) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        cmd.copy(tableName = newTable)

      case cmd@CarbonAlterTableDropPartitionCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(databaseName =
          Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(model = newModel)

      case cmd@CarbonAlterTableSplitPartitionCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(databaseName =
          Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(splitPartitionModel = newModel)

      case cmd@CarbonShowCarbonPartitionsCommand(tableIdentifier) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(tableIdentifier = new TableIdentifier(tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      //////////////////////////////////////////////////
      //                   Cache                      //
      //////////////////////////////////////////////////

      case cmd@CarbonDropCacheCommand(tableIdentifier, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(tableIdentifier = new TableIdentifier(tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonShowCacheCommand(tableOp, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      //////////////////////////////////////////////////
      //                   DataMap                    //
      //////////////////////////////////////////////////

      case cmd@CarbonCreateDataMapCommand(_, tableOp, _, _, _, _, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDropDataMapCommand(_, _, tableOp, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(table = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDataMapShowCommand(tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDataMapRebuildCommand(_, tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      //////////////////////////////////////////////////
      //               Data Management                //
      //////////////////////////////////////////////////

      case cmd@CarbonCleanFilesCommand(databaseNameOp, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonCliCommand(databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonDeleteLoadByIdCommand(_, databaseNameOp, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonDeleteLoadByLoadDateCommand(databaseNameOp, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonInsertColumnsCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(dbName = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonShowLoadsCommand(databaseNameOp, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@RefreshCarbonTableCommand(databaseNameOp, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonProjectForDeleteCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonProjectForUpdateCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))


      //////////////////////////////////////////////////
      //                   Streaming                  //
      //////////////////////////////////////////////////

      case cmd@CarbonCreateStreamCommand(_, sinkDbNameOp, _, _, _, _) =>
        requireDBNameNonEmpty(sinkDbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(sinkDbName = Some(LeoDatabase.convertUserDBNameToLeo(sinkDbNameOp.get)))

      case cmd@CarbonDropStreamCommand(_, _) =>
        cmd

      case cmd@CarbonShowStreamsCommand(tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableOp = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case plan => plan
    }

    (Some(updatedPlan), "")
  }

  private def requireDBNameNonEmpty(tableIdentifier: TableIdentifier): Option[String] = {
    if (tableIdentifier.database.isEmpty) {
      return Some("database name must be specified")
    } else if (tableIdentifier.database.get.equals("default")) {
      return Some("default database is not allowed, create a new database")
    }
    None
  }

  private def requireDBNameNonEmpty(databaseNameOp: Option[String]): Option[String] = {
    if (databaseNameOp.isEmpty) {
      return Some("database name must be specified")
    } else if (databaseNameOp.get.equals("default")) {
      return Some("default database is not allowed, create a new database")
    }
    None
  }
}
