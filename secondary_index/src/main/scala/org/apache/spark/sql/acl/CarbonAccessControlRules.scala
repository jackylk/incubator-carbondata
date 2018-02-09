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
package org.apache.spark.sql.acl

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.{CreateIndexTable, DropIndex}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableDataTypeChangeCommand, CarbonAlterTableDropColumnCommand, CarbonAlterTableRenameCommand}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.hive.acl.{HiveACLInterface, ObjectType, PrivObject, PrivType}
import org.apache.spark.sql.hive.execution.command.CarbonDropDatabaseCommand
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.processing.merger.CompactionType

private[sql] case class CarbonAccessControlRules(sparkSession: SparkSession,
    hCatalog: SessionCatalog,
    aclInterface: HiveACLInterface)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    if (ACLFileUtils.isSecureModeEnabled) {
      plan match {
        case c@CarbonCreateTableCommand(tableInfo: TableInfo, _, _, _) =>
          var databaseOpt : Option[String] = None
          if(tableInfo.getDatabaseName != null) {
            databaseOpt = Some(tableInfo.getDatabaseName)
          }
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            CarbonEnv.getDatabaseName(databaseOpt)(sparkSession),
            null,
            null,
            Set(PrivType.CREATE_NOGRANT))))

        case c@CarbonCreateDataMapCommand(dataMapName: String,
        tableIdentifier: TableIdentifier,
        dmClassName: String,
        dmproperties: Map[String, String],
        queryString: Option[String],
        ifNotExistsSet: Boolean) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession),
            tableIdentifier.table,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CreateIndexTable(indexModel, _, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(indexModel.databaseName)(sparkSession),
            indexModel.tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonLoadDataCommand(dbNameOp: Option[String],
        tableName: String, _, _, _, _, _, _, _, _, _, _, _, _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.INSERT_NOGRANT)
        case c@InsertIntoCarbonTable(relation, _, _, _, _) =>
          checkPrivilegeRecursively(c,
            Some(relation.carbonTable.getDatabaseName),
            relation.carbonTable.getTableName,
            PrivType.INSERT_NOGRANT)

        case c@CarbonDeleteLoadByIdCommand(_,
        dbNameOp: Option[String],
        tableName: String) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.DELETE_NOGRANT)

        case c@CarbonDeleteLoadByLoadDateCommand(dbNameOp: Option[String],
        tableName: String,
        _,
        _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.DELETE_NOGRANT)

        case c@DropIndex(_, dbNameOp: Option[String], tableName: String,
        parentTableName: String, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            parentTableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonDropDataMapCommand(
        dataMapName: String,
        ifExistsSet: Boolean,
        databaseNameOp: Option[String],
        tableName: String,
        forceDrop: Boolean) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@DropTableCommand(identifier, ifExists, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(identifier.database)(sparkSession),
            identifier.table,
            null,
            Set(PrivType.OWNER_PRIV))),
            identifier,
            ifExists)

        case c@DropDatabaseCommand(dbName, ifExists, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            dbName,
            null,
            null,
            Set(PrivType.OWNER_PRIV))),
            null,
            ifExists)

          hCatalog.listTables(dbName).foreach(tableIdentifier =>
            checkPrivilege(c, Set(new PrivObject(
              ObjectType.TABLE,
              CarbonEnv.getDatabaseName(Some(dbName))(sparkSession),
              tableIdentifier.table,
              null,
              Set(PrivType.OWNER_PRIV))))
          )
          c

        case c@CarbonDropTableCommand(_,
        dbNameOp: Option[String],
        tableName: String, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonShowLoadsCommand(dbNameOp, tableName, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.SELECT_NOGRANT))))

        case c@ShowIndexesCommand(dbNameOp, tableName, showIndexSql) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.SELECT_NOGRANT))))

        case c@CarbonDataMapShowCommand(dbNameOp, tableName) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)),
            tableName,
            PrivType.SELECT_NOGRANT)

        case c@DescribeTableCommand(identifier, _, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(identifier.database)(sparkSession),
            identifier.table,
            null,
            Set(PrivType.SELECT_NOGRANT))),
            identifier)

        case c@CarbonCleanFilesCommand(dbNameOp: Option[String], tableName: Option[String], _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName.getOrElse(""), PrivType.DELETE_NOGRANT)

        case c@CarbonAlterTableCompactionCommand(alterTableModel, tableInfoOp, _) =>
          // for executing close streaming compaction user should have owner Privilege
          val compactionType = CompactionType.valueOf(alterTableModel.compactionType.toUpperCase)
          if (compactionType == CompactionType.CLOSE_STREAMING) {
            checkPrivilegeRecursively(c,
              alterTableModel.dbName,
              alterTableModel.tableName,
              PrivType.OWNER_PRIV)
          }
          else {
            checkPrivilegeRecursively(c,
              alterTableModel.dbName,
              alterTableModel.tableName,
              PrivType.INSERT_NOGRANT)
          }

        case c@CarbonDropDatabaseCommand(command) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            command.databaseName,
            null,
            null,
            Set(PrivType.OWNER_PRIV))),
            null,
            command.ifExists)

        case c@CarbonAlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(alterTableChangeDataTypeModel.databaseName)(sparkSession)),
            alterTableChangeDataTypeModel.tableName,
            PrivType.OWNER_PRIV)

        case c@CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(alterTableAddColumnsModel.databaseName)(sparkSession),
            alterTableAddColumnsModel.tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonAlterTableDropColumnCommand(alterTableDropColumnModel) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(alterTableDropColumnModel.databaseName)(sparkSession)),
            alterTableDropColumnModel.tableName,
            PrivType.OWNER_PRIV)

        case c@CarbonAlterTableRenameCommand(alterTableRenameModel) =>
          checkPrivilegeRecursively(c,
            Some(CarbonEnv
              .getDatabaseName(alterTableRenameModel.oldTableIdentifier.database)(sparkSession)),
            alterTableRenameModel.oldTableIdentifier.table,
            PrivType.OWNER_PRIV)
        case s@CarbonAlterTableFinishStreaming(dbName, tableName) =>
          checkPrivilege(s, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbName)(sparkSession),
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))))
        case a@AlterTableSetPropertiesCommand(tableName, properties, isView) =>
          checkPrivilege(a, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(tableName.database)(sparkSession),
            tableName.table,
            null,
            Set(PrivType.OWNER_PRIV))))
        case u@UpdateTable(table, cols, _, sel, where) =>
          checkPrivilegeRecursively(u,
            Some(CarbonEnv.getDatabaseName(table.tableIdentifier.database)(sparkSession)),
            table.tableIdentifier.table,
            PrivType.UPDATE_NOGRANT)

        case d@DeleteRecords(statement, _, table) =>
          checkPrivilegeRecursively(d,
            Some(CarbonEnv.getDatabaseName(table.tableIdentifier.database)(sparkSession)),
            table.tableIdentifier.table,
            PrivType.DELETE_NOGRANT)

        case l: LogicalPlan => l
      }
    } else {
      plan
    }
  }

  private def checkPrivilege(
      l: LogicalPlan,
      privSet: Set[PrivObject],
      tableIdentifier: TableIdentifier = null,
      ifExists: Boolean = false): LogicalPlan = {
    val newSet = new mutable.HashSet[PrivObject]()
    for (priv <- privSet) {
      if (priv.objType == ObjectType.COLUMN || priv.objType == ObjectType.TABLE) {
        if (tableIdentifier == null) {
          newSet += priv
        } else if (!hCatalog.isTemporaryTable(tableIdentifier)) {
          newSet += priv
        }
      } else {
        newSet += priv
      }
    }
    if (aclInterface.checkPrivilege(newSet.toSet, ifExists)) {
      l
    } else {
      throw new AnalysisException("Missing Privileges", l.origin.line, l.origin.startPosition)
    }
  }

  private def checkPrivilegeRecursively(plan: LogicalPlan,
      dbNameOp: Option[String],
      tableName: String,
      privType: PrivType.PrivType): LogicalPlan = {
    checkPrivilege(plan, Set(new PrivObject(
      ObjectType.TABLE,
      CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
      tableName,
      null,
      Set(privType))))
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(dbNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
      .carbonTable
      val tableList = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
      if (!tableList.isEmpty) {
        tableList.asScala.foreach { tableName =>
          checkPrivilege(plan, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(privType))))
        }
      }
      val preAggregateTableList = carbonTable.getTableInfo.getDataMapSchemaList
      if (!preAggregateTableList.isEmpty) {
        preAggregateTableList.asScala.foreach { tableName =>
          checkPrivilege(plan, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName.getRelationIdentifier.getTableName,
            null,
            Set(privType))))
        }
      }
    plan
  }
}

