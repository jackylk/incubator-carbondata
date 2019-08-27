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
package org.apache.carbon.flink

import java.io.IOException
import java.util
import java.util.{Locale, Properties}

import com.google.gson.Gson
import org.apache.carbon.flink.adapter.ProxyRecoverableOutputStream
import org.apache.carbon.flink.writers._
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.FSDataOutputStream

/**
  * Note: Factory is serializable object. Can not define non-serializable field.
  */
class CarbonWriterFactory[T](databaseName: Option[String], tableName: String, tableProperties: String, writerProperties: Properties)
  extends BulkWriter.Factory[T] {

  @throws[IOException]
  override def create(outputStream: FSDataOutputStream): BulkWriter[T] = {
    if (!outputStream.isInstanceOf[ProxyRecoverableOutputStream]) {
      throw new IllegalArgumentException("Only support " + classOf[ProxyRecoverableOutputStream].getName + ".")
    }
    val table = CarbonWriterFactory.getTable(databaseName, tableName)
    val tableProperties = CarbonWriterFactory.getTableProperties(this.tableProperties)
    val writerType = writerProperties.getProperty(CarbonWriterProperty.TYPE)
    if (writerType == null) {
      throw new IllegalArgumentException("Writer property [" + CarbonWriterProperty.TYPE + "] is not set.")
    }
    val writer: BulkWriter[T] = writerType.toUpperCase(Locale.ENGLISH) match {
      case "LOCAL" => new LocalWriter[T](table, tableProperties, writerProperties)
      case "S3" => new S3Writer[T](table, tableProperties, writerProperties)
      case _ => throw new IllegalArgumentException("Unsupported writer type [" + writerType + "].")
    }
    outputStream.asInstanceOf[ProxyRecoverableOutputStream].setWriter(writer)
    writer
  }

}

object CarbonWriterFactory {

  private val GSON = new Gson

  private def getTable(databaseName: Option[String], tableName: String): CarbonTable = {
    CarbonMetadata.getInstance().getCarbonTable(databaseName.getOrElse("default"), tableName)
  }

  @SuppressWarnings(Array("unchecked"))
  private def getTableProperties(tableProperties: String): util.Map[String, String] = {
    GSON.fromJson(tableProperties, classOf[util.Map[_, _]]).asInstanceOf[util.Map[String, String]]
  }

}
