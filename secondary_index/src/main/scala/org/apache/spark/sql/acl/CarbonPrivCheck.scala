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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CarbonTableIdentifierImplicit, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{BatchedDataSourceScanExec, CodegenSupport, RowDataSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonInsertIntoCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableRenameCommand
import org.apache.spark.sql.execution.command.table.{CarbonDescribeFormattedCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.hive.acl.{HiveACLInterface, ObjectType, PrivObject, PrivType}
import org.apache.spark.sql.hive.execution.command.CarbonDropDatabaseCommand

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.scan.expression.{ColumnExpression, Expression}
import org.apache.carbondata.spark.rdd.CarbonScanRDD

private[sql] case class CarbonPrivCheck(sparkSession: SparkSession,
    hCatalog: SessionCatalog,
    aclInterface: HiveACLInterface)
  extends org.apache.spark.sql.catalyst.rules.Rule[SparkPlan] {

  val LOGGER = LogServiceFactory.getLogService("CarbonPrivCheck")

  override def apply(operator: SparkPlan): SparkPlan = checkPlan(operator)

  private def isSameTable(relation: CarbonDatasourceHadoopRelation,
      carbonInternalProject: Option[CarbonInternalProject]): Boolean = {
    isSameTable(relation.carbonRelation, carbonInternalProject)
  }

  private def isSameTable(relation: CarbonRelation,
      carbonInternalProject: Option[CarbonInternalProject]): Boolean = {
    carbonInternalProject.isDefined &&
    (TableIdentifier(relation.tableName, Some(relation.databaseName)) ==
     getTableIdentifier(carbonInternalProject.get.tableIdentifier))
  }

  private def getTableIdentifier(tableIdentifier: Seq[String]): TableIdentifier = {
    tableIdentifier match {
      case Seq(dbName, tableName) => TableIdentifier(tableName, Some(dbName))
      case Seq(tableName) =>
        TableIdentifier(tableName, Some(CarbonEnv.getDatabaseName(None)(sparkSession)))
      case _ => throw new IllegalArgumentException("invalid table identifier: " + tableIdentifier)
    }
  }

  private def checkPlan(plan: SparkPlan): SparkPlan = {
    if (ACLFileUtils.isSecureModeEnabled) {
      plan match {
        case c@ExecutedCommandExec(CarbonLoadDataCommand(dbNameOp: Option[String],
        tableName: String, _, _, _, _, _, _, _, _, _, _)) =>
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.INSERT_NOGRANT))))
        case c@ExecutedCommandExec(CarbonInsertIntoCommand(relation:
          CarbonDatasourceHadoopRelation,
        child: LogicalPlan,
        overwrite: Boolean, _)) =>
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(Some(relation.databaseName))(sparkSession),
            relation.tableName,
            null,
            Set(PrivType.INSERT_NOGRANT))))
        case c@ExecutedCommandExec(CarbonAlterTableRenameCommand(renameModel)) =>
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(renameModel.oldTableIdentifier.database)(sparkSession),
            renameModel.oldTableIdentifier.table,
            null,
            Set(PrivType.OWNER_PRIV))))
        case c@ExecutedCommandExec(CarbonDropTableCommand(_,
        dbNameOp: Option[String],
        tableName: String, _)) =>
          //          if (isIndexDrop) {
          //            doCheckPrivilege(c, Set(new PrivObject(
          //              ObjectType.TABLE,
          //              CarbonEnv.getDatabaseName(dbNameOp, sparkSession),
          //              parentTableName,
          //              null,
          //              Set(PrivType.OWNER_PRIV))))
          //          } else {
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))))
        //          }
        case c@ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) =>
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            drop.databaseName,
            null,
            null,
            Set(PrivType.OWNER_PRIV))),
            drop.ifExists)
        case c@ExecutedCommandExec(CarbonDescribeFormattedCommand(_, _, identifier)) =>
          doCheckPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(identifier.database)(sparkSession),
            identifier.table,
            null,
            Set(PrivType.SELECT_NOGRANT))))

        case sparkPlan =>
          var internalTable: Option[CarbonInternalProject] = None
          sparkPlan transformDown {
            case internalProject: CarbonInternalProject =>
              internalTable = Some(internalProject)
              internalProject
            case scan@BatchedDataSourceScanExec(projectList, rdd: CarbonScanRDD,
            relation: CarbonDatasourceHadoopRelation, _, _, _, logicalRelation)
              if !isSameTable(relation, internalTable) && logicalRelation.needPriv =>
              checkPrivilege(projectList, relation.carbonRelation, rdd)
              scan
            case scan@RowDataSourceScanExec(projectList, rdd: CarbonScanRDD,
            relation: CarbonDatasourceHadoopRelation, _, _, _, _)
              if !isSameTable(relation, internalTable) && scan.needPriv =>
              checkPrivilege(projectList, relation.carbonRelation, rdd)
              scan
            case scan@BatchedDataSourceScanExec(projectList, rdd: CarbonDecoderRDD,
            relation: CarbonDatasourceHadoopRelation, _, _, _, logicalRelation)
              if rdd.prev.isInstanceOf[CarbonScanRDD] &&
                 !isSameTable(relation, internalTable) && logicalRelation.needPriv =>
              checkPrivilege(projectList,
                relation.carbonRelation,
                rdd.prev.asInstanceOf[CarbonScanRDD])
              scan
            case scan@RowDataSourceScanExec(projectList, rdd: CarbonDecoderRDD,
            relation: CarbonDatasourceHadoopRelation, _, _, _, _)
              if rdd.prev.isInstanceOf[CarbonScanRDD] &&
                 !isSameTable(relation, internalTable) && scan.needPriv =>
              checkPrivilege(projectList,
                relation.carbonRelation,
                rdd.prev.asInstanceOf[CarbonScanRDD])
              scan
            //            case countStar@CarbonCountStar(_, relation: CarbonRelation, _, needPrev)
            //              if !isSameTable(relation, internalTable) && needPrev =>
            //              checkPrivilege(Seq.empty, relation, null)
            //              countStar
            case others => others
          }
      }
    } else {
      plan
    }
  }

  private def doCheckPrivilege(
      l: SparkPlan,
      privSet: Set[PrivObject],
      ifExists: Boolean = false): SparkPlan = {
    val newSet = new mutable.HashSet[PrivObject]()
    for (priv <- privSet) {
      if (priv.objType == ObjectType.COLUMN || priv.objType == ObjectType.TABLE) {
        if (!hCatalog
          .isTemporaryTable(CarbonTableIdentifierImplicit
            .toTableIdentifier(Seq(priv.db, priv.obj)))) {
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

  private def checkPrivilege(
      projectList: Seq[Attribute],
      relation: CarbonRelation,
      carbonScanRDD: CarbonScanRDD): Unit = {
    val (dbName, tblName) = (relation.databaseName, relation.tableName)
    LOGGER.info("Start Select query Acl privilege table level")
    if (!aclInterface.checkPrivilege(
      Set(new PrivObject(ObjectType.TABLE, dbName, tblName, null,
        Set(PrivType.SELECT_NOGRANT))))) {
      var projectSet = AttributeSet(projectList.flatMap(_.references)).map(a => a.name).toSet
      if (null != carbonScanRDD) {
        if (!carbonScanRDD.columnProjection.isEmpty) {
          projectSet = projectSet ++ carbonScanRDD.columnProjection.getAllColumns
        }
        if (carbonScanRDD.filterExpression != null) {
          carbonScanRDD.filterExpression.getChildren.asScala.map { expr =>
            projectSet = projectSet ++ getAllColumns(expr, projectSet)
          }
        }
      }
      if (projectSet.isEmpty) {
        throw new AnalysisException("Missing Privileges")
      }
      projectSet.foreach {
        att =>
          if (!(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_POSITIONID.equalsIgnoreCase(att) ||
                CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID.equalsIgnoreCase(att))) {
            LOGGER.info("Inside for Select query Acl privilege column level")
            if (!aclInterface.checkPrivilege(
              Set(new PrivObject(ObjectType.COLUMN, dbName, tblName, att,
                Set(PrivType.SELECT_NOGRANT))))) {
              throw new AnalysisException("Missing Privileges")
            }
            LOGGER.info("End Inside for Select query Acl privilege column level")
          }
      }
    }
    LOGGER.info("End Select query Acl privilege table level")
  }

  private def getAllColumns(expression: Expression, projectSet: Set[String]): Set[String] = {
    var newProjSet = projectSet
    if (expression.isInstanceOf[ColumnExpression]) {
      newProjSet = newProjSet. + (expression.asInstanceOf[ColumnExpression].getColumnName)
    } else {
      expression.getChildren.asScala.map { expr: Expression =>
        if (expr.isInstanceOf[ColumnExpression]) {
          newProjSet = newProjSet. + (expr.asInstanceOf[ColumnExpression].getColumnName)
        } else {
          newProjSet = newProjSet ++ getAllColumns(expr, newProjSet)
        }
      }
    }
    newProjSet
  }
}


