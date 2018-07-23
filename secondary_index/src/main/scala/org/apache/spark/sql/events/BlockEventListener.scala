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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException

import org.apache.carbondata.events._
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreExecutionEvent

/**
 * This class is added to listen for create and load events to block Bucketing, Partition,
 * Complex Datatypes and InsertOverwrite features
 */
class BlockEventListener extends OperationEventListener with Logging {

  override def onEvent(event: Event,
    operationContext: OperationContext): Unit = {
    event match {
      case createTablePreExecutionEvent: CreateTablePreExecutionEvent =>
        if (createTablePreExecutionEvent.tableInfo.isDefined) {
          val bucketInfo = createTablePreExecutionEvent.tableInfo.get.getFactTable.getBucketingInfo
          val partitionInfo = createTablePreExecutionEvent.tableInfo.get.getFactTable
            .getPartitionInfo
          val fields = createTablePreExecutionEvent.tableInfo.get.getFactTable.getListOfColumns
          if (null != bucketInfo) {
            throw new AnalysisException("Bucketing feature is not supported")
          }
          if (null != partitionInfo) {
            throw new AnalysisException("Partition feature is not supported")
          }
          fields.asScala.foreach(field => {
            val dataType = field.getDataType.getName
            if (dataType.equalsIgnoreCase("array") || dataType.equalsIgnoreCase("struct")) {
              throw new AnalysisException("Complex DataTypes not supported")
            }
          })
        }
      case loadTablePreExecutionEvent: LoadTablePreExecutionEvent =>
        val isOverWrite = loadTablePreExecutionEvent.isOverWriteTable
        val options = loadTablePreExecutionEvent.getUserProvidedOptions.asScala
        if (isOverWrite) {
          throw PreEventException("Insert Overwrite is not supported", false)
        }
        if (options.nonEmpty &&
            options.exists(_._1.equalsIgnoreCase("COMPLEX_DELIMITER_LEVEL_1")) ||
            options.exists(_._1.equalsIgnoreCase("COMPLEX_DELIMITER_LEVEL_2"))) {
          throw PreEventException(
            "Invalid load Options, Complex DataTypes not supported", false)
        }
    }

  }
}
