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
import org.apache.spark.sql.execution.RowDataSourceScanExec

object SparkObjectCreationHelper {

  def getOutputObjectFromRowDataSourceScan(scan: RowDataSourceScanExec): Seq[Attribute] = {
    scan.output
  }
}
