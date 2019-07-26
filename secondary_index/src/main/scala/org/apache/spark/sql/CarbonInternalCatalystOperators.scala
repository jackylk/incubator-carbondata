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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Shows Indexes in a table
 */
case class ShowIndexesCommand(databaseNameOp: Option[String],
    table: String,
    var showIndexSql: String = null)
  extends Command {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("Index Table Name", StringType, nullable = false)(),
      AttributeReference("Index Status", StringType, nullable = false)(),
      AttributeReference("Indexed Columns", StringType, nullable = false)())
  }
}

/**
 * Drop index in a table
 */
case class DropIndexCommand(ifExistsSet: Boolean, databaseNameOp: Option[String],
    tableName: String, parentTableName: String, var dropIndexSql: String = null)
  extends Command {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }
}
