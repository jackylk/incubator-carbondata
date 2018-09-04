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
package org.apache.spark.sql.helper

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{CreateFileFormatContext, CreateHiveTableContext, LocationSpecContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.parser.CarbonHelperACLSqlAstBuilder

object SparkObjectCreationHelper {

  def getOutputObjectFromRowDataSourceScan(scan: RowDataSourceScanExec): Seq[Attribute] = {
    scan.fullOutput
  }

  def createTableTuple(ctx: CreateHiveTableContext,
    helper: CarbonHelperACLSqlAstBuilder, fileStorage: String): LogicalPlan = {
    val createTableTuple = (ctx.createTableHeader, ctx.skewSpec(0),
      ctx.bucketSpec(0), ctx.partitionColumns, ctx.columns, ctx.tablePropertyList(0), ctx
      .locationSpec(0),
      Option(ctx.STRING(0)).map(string), ctx.AS, ctx.query, fileStorage)
    helper.createCarbonTable(createTableTuple)
  }

  def getFileFormat(ctx: CreateHiveTableContext): CreateFileFormatContext = {
    ctx.createFileFormat(0)
  }

  def getLocationSpec(ctx: CreateHiveTableContext): LocationSpecContext = {
    ctx.locationSpec(0)
  }

}
