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
package org.apache.spark.sql.execution.command.datamap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.dev.DataMapFactory
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.{DataMapManager, IndexDataMapProvider}

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    tableIdentifier: Option[TableIdentifier],
    dmProviderName: String,
    dmProperties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false)
  extends AtomicRunnableCommand {

  private var dataMapProvider: DataMapProvider = _
  private var mainTable: CarbonTable = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    mainTable = tableIdentifier match {
      case Some(table) =>
        CarbonEnv.getCarbonTable(table.database, table.table)(sparkSession)
      case _ => null
    }

    if (mainTable != null && !mainTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (mainTable != null && mainTable.getDataMapSchema(dataMapName) != null) {
      if (!ifNotExistsSet) {
        throw new MalformedDataMapCommandException(s"DataMap name '$dataMapName' already exist")
      } else {
        return Seq.empty
      }
    }

    if (mainTable != null &&
        mainTable.isStreamingTable &&
        !(dmProviderName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.toString)
          || dmProviderName.equalsIgnoreCase(DataMapClassProvider.TIMESERIES.toString))) {
      throw new MalformedCarbonCommandException(s"Streaming table does not support creating " +
                                                s"$dmProviderName datamap")
    }

    dataMapSchema = new DataMapSchema(dataMapName, dmProviderName)
    dataMapSchema.setProperties(new java.util.HashMap[String, String](
      dmProperties.map(x => (x._1.trim, x._2.trim)).asJava))
    dataMapProvider = DataMapManager.get.getDataMapProvider(mainTable, dataMapSchema, sparkSession)

    // If it is index datamap, check whether the column has datamap created already
    dataMapProvider match {
      case provider: IndexDataMapProvider =>
        val datamaps = DataMapStoreManager.getInstance.getAllDataMap(mainTable).asScala
        val existingIndexColumn = mutable.Set[String]()
        datamaps.foreach { datamap =>
          datamap.getDataMapSchema.getIndexColumns.foreach(existingIndexColumn.add)
        }

        provider.getIndexedColumns.asScala.foreach { column =>
          if (existingIndexColumn.contains(column.getColName)) {
            throw new MalformedDataMapCommandException(String.format(
              "column '%s' already has datamap created", column.getColName))
          }
        }
        dataMapProvider.initMeta(queryString.orNull)
        DataMapStatusManager.disableDataMap(dataMapName)
      case _ =>
        dataMapProvider.initMeta(queryString.orNull)
    }
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    LOGGER.audit(s"DataMap $dataMapName successfully added")
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.initData()
      if (mainTable != null &&
          mainTable.isAutoRefreshDataMap &&
          !dataMapSchema.isIndexDataMap) {
        dataMapProvider.rebuild()
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dataMapProvider != null) {
      dataMapProvider.cleanMeta()
    }
    Seq.empty
  }
}

