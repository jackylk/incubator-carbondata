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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.spark.indextable.IndexTableUtil

/**
 *
 */
object CarbonInternalHiveMetadataUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def retrieveRelation(plan: LogicalPlan): CarbonDatasourceHadoopRelation = {
    plan match {
      case SubqueryAlias(alias, l: LogicalRelation, _) if (l.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case l: LogicalRelation if (l.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case _ => null
    }
  }

  def refreshTable(dbName: String, tableName: String, sparkSession: SparkSession): Unit = {
    val tableWithDb = dbName + "." + tableName
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableWithDb)
    sparkSession.sessionState.catalog.refreshTable(tableIdent)
  }

  /**
   * This method invalidates the table from HiveMetastoreCatalog before dropping table and also
   * removes the index table info from parent carbon table.
   *
   * @param indexTableIdentifier
   * @param indexInfo
   * @param parentCarbonTable
   * @param sparkSession
   */
  def invalidateAndUpdateIndexInfo(indexTableIdentifier: TableIdentifier,
      indexInfo: String, parentCarbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val dbName = indexTableIdentifier.database
      .getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME)
    val tableName = indexTableIdentifier.table
    try {
      if (indexInfo != null) {
        val parentTableName = parentCarbonTable.getTableName
        val newIndexInfo = IndexTableUtil.removeIndexTable(indexInfo, dbName, tableName)
        CarbonInternalScalaUtil.removeIndexTableInfo(parentCarbonTable, tableName)
        sparkSession.sql(
          s"""ALTER TABLE $dbName.$parentTableName SET SERDEPROPERTIES ('indexInfo'
             |='$newIndexInfo')"""
            .stripMargin)
        FileInternalUtil.touchSchemaFileTimestamp(dbName,
          parentTableName,
          parentCarbonTable.getTablePath,
          System.currentTimeMillis())
        FileInternalUtil.
          touchStoreTimeStamp()
        refreshTable(dbName, parentTableName, sparkSession)
      }
    } catch {
      case e: Exception =>
        LOGGER.audit(
          s"Error While deleting the table $dbName.$tableName during drop carbon table" +
          e.getMessage)
    }
  }

  def transformToRemoveNI(expression: Expression): Expression = {
    expression.transform {
      case hiveUDF: HiveSimpleUDF if hiveUDF.function.isInstanceOf[NonIndexUDFExpression] =>
        hiveUDF.asInstanceOf[HiveSimpleUDF].children.head
    }
  }

  def checkNIUDF(condition: Expression): Boolean = {
    condition match {
      case hiveUDF: HiveSimpleUDF if (hiveUDF.function.isInstanceOf[NonIndexUDFExpression]) => true
      case _ => false
    }
  }

}

class NonIndexUDFExpression extends UDF {
  def evaluate(input: Any): Boolean = true
}
