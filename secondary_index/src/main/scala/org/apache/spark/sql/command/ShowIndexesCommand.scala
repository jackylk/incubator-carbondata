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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.CarbonInternalMetastore
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.CarbonInternalScalaUtil

/**
 * Command to list the indexes for a table
 */
private[sql] case class ShowIndexes(
    databaseNameOp: Option[String],
    tableName: String,
    var inputSqlString: String = null,
    override val output: Seq[Attribute]) extends RunnableCommand {


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    // Here using checkSchemasModifiedTimeAndReloadTables in tableExists to reload metadata if
    // schema is changed by other process, so that tableInfoMap woulb be refilled.
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val identifier = TableIdentifier(tableName, databaseNameOp)
    val tableExists = catalog
      .tableExists(identifier)(sparkSession)
    if (!tableExists) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val carbonTable = catalog.lookupRelation(Some(databaseName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    if (carbonTable == null) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
    if (null == indexesMap) {
      throw new Exception("Secondary index information is not loaded in main table")
    }
    val indexTableMap = indexesMap.asScala
    if (indexTableMap.nonEmpty) {
      val indexList = indexTableMap.map { indexInfo =>
        val isSITableEnabled = sparkSession.sessionState.catalog
          .getTableMetadata(TableIdentifier(indexInfo._1, Some(databaseName))).storage.properties
          .getOrElse("isSITableEnabled", "true").equalsIgnoreCase("true")
        if (isSITableEnabled) {
          (indexInfo._1, indexInfo._2.asScala.mkString(","), "enabled")
        } else {
          (indexInfo._1, indexInfo._2.asScala.mkString(","), "disabled")
        }
      }
      indexList.map { case (indexTableName, columnName, isSITableEnabled) =>
        Row(indexTableName, isSITableEnabled, columnName)
      }.toSeq
    } else {
      Seq.empty
    }
  }

}
