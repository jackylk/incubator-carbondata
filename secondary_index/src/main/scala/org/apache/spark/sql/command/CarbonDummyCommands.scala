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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * dummy command
 */
private[sql] case class CarbonDummyCommand()
  extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq()
  }
}
