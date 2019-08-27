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

import java.io.{File, FileInputStream}
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import com.obs.services.ObsClient
import org.apache.carbon.flink.{CarbonWriter, CarbonWriterProperty}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.sdk.file.Schema
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.management.CarbonAddLoadCommand

/**
  * The writer which write flink data to S3(OBS) carbon file.
  *
  * @param table            carbon table metadata
  * @param tableProperties  carbon table properties
  * @param writerProperties writer properties
  */
class S3Writer[T](table: CarbonTable, tableProperties: java.util.Map[String, String], writerProperties: Properties)
  extends CarbonWriter[T](table, tableProperties) {

  private var writePartition: String = _

  private var writePath: String = _

  private var writer: org.apache.carbondata.sdk.file.CarbonWriter = _

  this.initialize()

  private def initialize(): Unit = {
    if (S3Writer.LOGGER.isDebugEnabled) {
      S3Writer.LOGGER.debug("Open writer. " + this.toString)
    }
    val outputRootPathLocal = writerProperties.getProperty(CarbonWriterProperty.LOCAL_OUTPUT_PATH)
    if (outputRootPathLocal == null) {
      throw new IllegalArgumentException("Writer property [" + CarbonWriterProperty.LOCAL_OUTPUT_PATH + "] is not set.")
    }
    val writePartition = System.currentTimeMillis() + "_" + S3Writer.WRITER_SERIAL_NUMBER.incrementAndGet()
    val writePath = outputRootPathLocal + "_" + writePartition + "/"
    val tableCloned = CarbonTable.buildFromTableInfo(TableInfo.deserialize(table.getTableInfo.serialize()))
    tableCloned.getTableInfo.setTablePath(writePath)
    this.writePartition = writePartition
    this.writePath = writePath
    this.writer = org.apache.carbondata.sdk.file.CarbonWriter.builder
      .outputPath("")
      .writtenBy("flink")
      .withTable(tableCloned)
      .withTableProperties(tableProperties)
      .withJsonInput(S3Writer.getTableSchema(tableCloned))
      .build
  }

  override def addElement(element: T): Unit = {
    this.writer.write(element)
  }

  override def flush(): Unit = {
    // to do nothing.
  }

  override def finish(): Unit = {
    if (S3Writer.LOGGER.isDebugEnabled) {
      S3Writer.LOGGER.debug("Close writer. " + this.toString)
    }
    try {
      val s3AccessKey = writerProperties.getProperty(S3Writer.S3_ACCESS_KEY)
      val s3SecretKey = writerProperties.getProperty(S3Writer.S3_SECRET_KEY)
      val s3Endpoint = writerProperties.getProperty(S3Writer.S3_ENDPOINT)
      val s3Bucket = writerProperties.getProperty(S3Writer.S3_BUCKET)
      val s3RootPath = writerProperties.getProperty(S3Writer.S3_ROOT_PATH, "")
      if (s3AccessKey == null) {
        throw new IllegalArgumentException("Writer property [" + S3Writer.S3_ACCESS_KEY + "] is not set.")
      }
      if (s3SecretKey == null) {
        throw new IllegalArgumentException("Writer property [" + S3Writer.S3_SECRET_KEY + "] is not set.")
      }
      if (s3Endpoint == null) {
        throw new IllegalArgumentException("Writer property [" + S3Writer.S3_ENDPOINT + "] is not set.")
      }
      if (s3Bucket == null) {
        throw new IllegalArgumentException("Writer property [" + S3Writer.S3_BUCKET + "] is not set.")
      }
      val outputRootPathRemote = s3RootPath + this.writePartition + "/"
      val client: ObsClient = new ObsClient(s3AccessKey, s3SecretKey, s3Endpoint)
      try {
        this.writer.close()
        this.addSegmentFiles(client, this.writePath + "Fact/Part0/Segment_null/", s3Bucket, outputRootPathRemote)
        this.addSegment(buildConfiguration(s3AccessKey, s3SecretKey, s3Endpoint), s3Bucket, outputRootPathRemote)
      } finally {
        client.close()
      }
    } finally {
      FileUtils.deleteDirectory(new File(this.writePath))
    }
  }

  /**
    * Upload local segment files to OBS.
    */
  private def addSegmentFiles(client: ObsClient, localPath: String, remoteBucket: String, remotePath: String): Unit = {
    val files = new File(localPath).listFiles()
    if (files == null) {
      return
    }
    for (file <- files) {
      val inputStream = new FileInputStream(file.getAbsolutePath)
      try {
        if (S3Writer.LOGGER.isDebugEnabled) {
          S3Writer.LOGGER.debug(s"Upload file[${file.getAbsoluteFile}] to OBS start.")
        }
        client.putObject(remoteBucket, remotePath + file.getName, inputStream)
        if (S3Writer.LOGGER.isDebugEnabled) {
          S3Writer.LOGGER.debug(s"Upload file[${file.getAbsoluteFile}] to OBS end.")
        }
      } finally {
        inputStream.close()
      }
    }
  }

  /**
    * Add segment to carbon table. Modify carbon table metadata.
    */
  private def addSegment(configuration: Configuration, remoteBucket: String, remotePath: String): Unit = {
    ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo.getNonSerializableExtraInfo.put("carbonConf", configuration)
    val segmentLocation = CarbonCommonConstants.S3A_PREFIX + remoteBucket + "/" + remotePath
    val options = Map("path" -> segmentLocation, "format" -> "carbon")
    S3Writer.LOGGER.info(s"Add segment[$segmentLocation] to table ${table.getTableName}.")
    val command = CarbonAddLoadCommand(Option.apply(table.getDatabaseName), table.getTableName, Option.apply(options))
    this.getClass.synchronized {
      command.processMetadata(SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get))
    }
  }

  private def buildConfiguration(accessKey: String, secretKey: String, endpoint: String): Configuration = {
    val configuration = new Configuration(true)
    configuration.set("fs.s3a.access.key", accessKey)
    configuration.set("fs.s3a.secret.key", secretKey)
    configuration.set("fs.s3a.endpoint", endpoint)
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
    configuration.set("fs.obs.access.key", accessKey)
    configuration.set("fs.obs.secret.key", secretKey)
    configuration.set("fs.obs.endpoint", endpoint)
    configuration.set("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
    configuration
  }

}

object S3Writer {

  val S3_ACCESS_KEY = "carbon.writer.s3.access.key"

  val S3_SECRET_KEY = "carbon.writer.s3.secret.key"

  val S3_ENDPOINT = "carbon.writer.s3.endpoint"

  val S3_BUCKET = "carbon.writer.s3.bucket"

  val S3_ROOT_PATH = "carbon.writer.s3.root.path"

  private val LOGGER = LogServiceFactory.getLogService(S3Writer.getClass.getCanonicalName)

  private val WRITER_SERIAL_NUMBER = new AtomicLong(0)

  private def getTableSchema(table: CarbonTable): Schema = {
    val columnList = table.getCreateOrderColumn(table.getTableName)
    val columnSchemaList = new util.ArrayList[ColumnSchema]
    import scala.collection.JavaConversions._
    for (column <- columnList) {
      columnSchemaList.add(column.getColumnSchema)
    }
    new Schema(columnSchemaList)
  }

}
