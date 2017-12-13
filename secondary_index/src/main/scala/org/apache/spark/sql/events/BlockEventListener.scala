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

package org.apache.spark.sql.events

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException

import org.apache.carbondata.events._

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
        val options = loadTablePreExecutionEvent.userProvidedOptions
        if (isOverWrite) {
          throw new AnalysisException("Insert Overwrite is not supported")
        }
        if (options.nonEmpty &&
            options.exists(_._1.equalsIgnoreCase("COMPLEX_DELIMITER_LEVEL_1")) ||
            options.exists(_._1.equalsIgnoreCase("COMPLEX_DELIMITER_LEVEL_2"))) {
          throw new AnalysisException(
            "Invalid load Options, Complex DataTypes not supported")
        }
    }

  }
}
