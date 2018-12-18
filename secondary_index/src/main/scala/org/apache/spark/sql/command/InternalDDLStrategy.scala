/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.command

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.{DropIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.ExecutedCommandExec

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException


/**
 * carbon strategy for internal DDL command
 */
class InternalDDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CreateIndexTable(indexModel, tableProperties, createIndexSql, isCreateSIndex) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
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
          CarbonEnv.getInstance(sparkSession).carbonMetaStore
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
          val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .tableExists(tableIdentifier)(sparkSession)
          if (isCarbonTable) {
            val isIndexTableExist = CarbonEnv.getInstance(sparkSession).carbonMetaStore
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
