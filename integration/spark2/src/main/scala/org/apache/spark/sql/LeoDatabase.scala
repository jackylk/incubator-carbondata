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
import org.apache.spark.sql.execution.datasources.CreateTable

object LeoDatabase {
  var DEFAULT_PROJECTID: String = "_default_projectid"

  def fromUserTable(userTable: TableIdentifier): TableIdentifier = {
    TableIdentifier(
      userTable.table,
      Some(fromUserDBName(userTable.database.get)))
  }

  def fromUserDBName(userDBName: String): String = {
    getLeoDBPrefix + userDBName
  }

  def extractUserDBName(leoDBName: String): String = {
    leoDBName.substring(DEFAULT_PROJECTID.length + 1)
  }

  def getLeoDBPrefix = DEFAULT_PROJECTID + "_"


  /**
   * Return false if plan is invalid, otherwise return updated plan after
   * modifying database name that user gives.
   *
   * Following case the plan is invalid:
   * 1. user does not give database name
   * 2. USE DATABASE command
   */
  def replaceAllDBName(parsedPlan: LogicalPlan): (Option[LogicalPlan], String) = {
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
          UnresolvedRelation(LeoDatabase.fromUserTable(relation.tableIdentifier))
        }

      case cmd@CreateDatabaseCommand(databaseName, ifNotExists, path, comment, props) =>
        if (databaseName.equals("default")) {
          return (None, "database name default is not allowed")
        }
        val db = LeoDatabase.fromUserDBName(databaseName)
        CreateDatabaseCommand(db, ifNotExists, path, comment, props)

      case cmd@DropDatabaseCommand(databaseName, ifExists, cascade) =>
        val db = LeoDatabase.fromUserDBName(databaseName)
        DropDatabaseCommand(db, ifExists, cascade)

      case cmd@CreateTable(table, saveMode, query) =>
        if (table.identifier.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (table.identifier.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        val newTable = new TableIdentifier(
          table.identifier.table,
          Some(LeoDatabase.fromUserDBName(table.identifier.database.get)))
        CreateTable(table.copy(identifier = newTable), saveMode, query)

      case cmd@DropTableCommand(table, ifExists, isView, purge) =>
        if (table.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (table.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        val newTable = new TableIdentifier(
          table.table,
          Some(LeoDatabase.fromUserDBName(table.database.get)))
        DropTableCommand(newTable, ifExists, isView, purge)

      case cmd@SetDatabaseCommand(_) =>
        return (None, "use database command is not allowed, " +
                      "specify database name in the query instead")

      case cmd@ShowDatabasesCommand(databasePattern) =>
        if (databasePattern.isDefined) {
          return (None, "database pattern is not supported")
        }
        ShowDatabasesCommand(Some(LeoDatabase.getLeoDBPrefix + "*"))

      case cmd@ShowTablesCommand(dbNameOp, t, isExtended, partitionSpec) =>
        if (dbNameOp.isEmpty) {
          return (None, "database name must be specified")
        } else if (dbNameOp.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        ShowTablesCommand(
          Some(LeoDatabase.fromUserDBName(dbNameOp.get)), t, isExtended, partitionSpec)

      case cmd@DescribeTableCommand(table, partitionSpec, isExtended) =>
        if (table.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (table.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        DescribeTableCommand(LeoDatabase.fromUserTable(table), partitionSpec, isExtended)

      case cmd@ExplainCommand(plan, extended, codegen, cost) =>
        val (newPlanOp, msg) = replaceAllDBName(plan)
        if (newPlanOp.isEmpty) {
          return (None, msg)
        }
        ExplainCommand(newPlanOp.get, extended, codegen, cost)

      case plan => plan
    }

    (Some(updatedPlan), "")
  }

}
