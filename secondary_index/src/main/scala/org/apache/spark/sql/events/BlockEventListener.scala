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
package org.apache.spark.sql.events

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException

import org.apache.carbondata.events._

/**
 * This class is added to listen for create event to block Bucketing feature
 */
class BlockEventListener extends OperationEventListener with Logging {

  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    event match {
      case createTablePreExecutionEvent: CreateTablePreExecutionEvent =>
        if (createTablePreExecutionEvent.tableInfo.isDefined) {
          val bucketInfo = createTablePreExecutionEvent.tableInfo.get.getFactTable.getBucketingInfo
          if (null != bucketInfo) {
            throw new AnalysisException("Bucketing feature is not supported")
          }
        }
    }
  }
}
