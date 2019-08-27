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
package org.apache.carbon.flink.writers

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import org.apache.carbon.flink.{CarbonWriter, CarbonWriterProperty}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.sdk.file.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.management.CarbonAddLoadCommand

/**
  * The writer which write flink data to local carbon file.
  *
  * @param table            carbon table metadata
  * @param tableProperties  carbon table properties
  * @param writerProperties local writer properties
  */
class LocalWriter[T](table: CarbonTable, tableProperties: java.util.Map[String, String], writerProperties: Properties)
  extends CarbonWriter[T](table, tableProperties) {

  private var writePartition: String = _

  private var writePath: String = _

  private var writer: org.apache.carbondata.sdk.file.CarbonWriter = _

  this.initialize()

  private def initialize(): Unit = {
    if (LocalWriter.LOGGER.isDebugEnabled) {
      LocalWriter.LOGGER.debug("Open writer. " + this.toString)
    }
    val outputRootPath = writerProperties.getProperty(CarbonWriterProperty.LOCAL_OUTPUT_PATH)
    if (outputRootPath == null) {
      throw new IllegalArgumentException("Writer property [" + CarbonWriterProperty.LOCAL_OUTPUT_PATH + "] is not set.")
    }
    val writePartition = System.currentTimeMillis() + "_" + LocalWriter.WRITER_SERIAL_NUMBER.incrementAndGet()
    val writePath = outputRootPath + "_" + writePartition + "/"
    val tableCloned = CarbonTable.buildFromTableInfo(TableInfo.deserialize(table.getTableInfo.serialize()))
    tableCloned.getTableInfo.setTablePath(writePath)
    this.writePartition = writePartition
    this.writePath = writePath
    this.writer = org.apache.carbondata.sdk.file.CarbonWriter.builder
      .outputPath("")
      .writtenBy("flink")
      .withTable(tableCloned)
      .withTableProperties(tableProperties)
      .withJsonInput(LocalWriter.toTableSchema(tableCloned))
      .build
  }

  override def addElement(element: T): Unit = {
    this.writer.write(element)
  }

  override def flush(): Unit = {
    // to do nothing.
  }

  override def finish(): Unit = {
    if (LocalWriter.LOGGER.isDebugEnabled) {
      LocalWriter.LOGGER.debug("Close writer. " + this.toString)
    }
    this.writer.close()
    this.addSegment()
  }

  private def addSegment(): Unit = {
    val segmentLocation = this.writePath + "Fact/Part0/Segment_null/"
    val options = Map("path" -> segmentLocation, "format" -> "carbon")
    LocalWriter.LOGGER.info(s"Add segment[$segmentLocation] to table ${table.getTableName}.")
    val command = CarbonAddLoadCommand(Option.apply(table.getDatabaseName), table.getTableName, Option.apply(options))
    command.processMetadata(SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get))
  }

}

object LocalWriter {

  private val LOGGER = LogServiceFactory.getLogService(LocalWriter.getClass.getCanonicalName)

  private val WRITER_SERIAL_NUMBER = new AtomicLong(0)

  private def toTableSchema(table: CarbonTable): Schema = {
    val columnList = table.getCreateOrderColumn(table.getTableName)
    val columnSchemaList = new util.ArrayList[ColumnSchema]
    import scala.collection.JavaConversions._
    for (column <- columnList) {
      columnSchemaList.add(column.getColumnSchema)
    }
    new Schema(columnSchemaList)
  }

}


