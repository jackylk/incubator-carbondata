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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.datamap.IndexDataMapProvider
import org.apache.carbondata.events._

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    table: TableIdentifier,
    dmProviderName: String,
    dmProperties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false,
    var deferredRebuild: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private var provider: IndexDataMapProvider = _
  private var mainTable: CarbonTable = _
  private var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    mainTable = CarbonEnv.getCarbonTable(table.database, table.table)(sparkSession)

    setAuditTable(mainTable)
    setAuditInfo(Map("provider" -> dmProviderName, "dmName" -> dataMapName) ++ dmProperties)

    if (!mainTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (mainTable.getDataMapSchema(dataMapName) != null) {
      if (!ifNotExistsSet) {
        throw new MalformedDataMapCommandException(s"DataMap name '$dataMapName' already exist")
      } else {
        return Seq.empty
      }
    }

    if (CarbonUtil.getFormatVersion(mainTable) != ColumnarFormatVersion.V3) {
      throw new MalformedCarbonCommandException(s"Unsupported operation on table with " +
                                                s"V1 or V2 format data")
    }

    dataMapSchema = new DataMapSchema(dataMapName, dmProviderName)

    val property = dmProperties.map(x => (x._1.trim, x._2.trim)).asJava
    val javaMap = new java.util.HashMap[String, String](property)
    javaMap.put(DataMapProperty.DEFERRED_REBUILD, deferredRebuild.toString)
    dataMapSchema.setProperties(javaMap)

    if (dataMapSchema.isIndexDataMap && mainTable == null) {
      throw new MalformedDataMapCommandException(
        "For this datamap, main table is required. Use `CREATE DATAMAP ... ON TABLE ...` ")
    }
    provider = new IndexDataMapProvider(mainTable, dataMapSchema, sparkSession)
    if (deferredRebuild && !provider.supportRebuild()) {
      throw new MalformedDataMapCommandException(
        s"DEFERRED REBUILD is not supported on this datamap $dataMapName" +
        s" with provider ${dataMapSchema.getProviderName}")
    }

    if (mainTable.isChildTableForMV) {
      throw new MalformedDataMapCommandException(
        "Cannot create DataMap on child table " + mainTable.getTableUniqueName)
    }

    val storeLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
    val operationContext: OperationContext = new OperationContext()

    // check whether the column has index created already
    val isBloomFilter = DataMapClassProvider.BLOOMFILTER.getShortName
      .equalsIgnoreCase(dmProviderName)
    val datamaps = DataMapStoreManager.getInstance.getAllDataMap(mainTable).asScala
    val thisDmProviderName = provider.getDataMapSchema.getProviderName
    val existingIndexColumn4ThisProvider = datamaps.filter { datamap =>
      thisDmProviderName.equalsIgnoreCase(datamap.getDataMapSchema.getProviderName)
    }.flatMap { datamap =>
      datamap.getDataMapSchema.getIndexColumns
    }.distinct

    provider.getIndexedColumns.asScala.foreach { column =>
      if (existingIndexColumn4ThisProvider.contains(column.getColName)) {
        throw new MalformedDataMapCommandException(String.format(
          "column '%s' already has %s index datamap created",
          column.getColName, thisDmProviderName))
      } else if (isBloomFilter) {
        if (column.getDataType == DataTypes.BINARY) {
          throw new MalformedDataMapCommandException(
            s"BloomFilter datamap does not support Binary datatype column: ${
              column.getColName
            }")
        }
        // if datamap provider is bloomfilter,the index column datatype cannot be complex type
        if (column.isComplex) {
          throw new MalformedDataMapCommandException(
            s"BloomFilter datamap does not support complex datatype column: ${
              column.getColName
            }")
        }
      }
    }

    val preExecEvent = CreateDataMapPreExecutionEvent(sparkSession, storeLocation, table)
    OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

    provider.initMeta(queryString.orNull)
    DataMapStatusManager.disableDataMap(dataMapName)

    val postExecEvent = CreateDataMapPostExecutionEvent(sparkSession,
      storeLocation, Some(table), dmProviderName)
    OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (provider != null) {
      provider.initData()
      if (!deferredRebuild) {
        provider.rebuild()
        val operationContext = new OperationContext()
        val storeLocation = CarbonProperties.getInstance().getSystemFolderLocation
        val preExecEvent = UpdateDataMapPreExecutionEvent(sparkSession, storeLocation, table)
        OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

        DataMapStatusManager.enableDataMap(dataMapName)

        val postExecEvent = UpdateDataMapPostExecutionEvent(sparkSession, storeLocation, table)
        OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
      }
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (provider != null) {
      val table = Some(TableIdentifier(mainTable.getTableName, Some(mainTable.getDatabaseName)))
      CarbonDropDataMapCommand(
        dataMapName,
        true,
        table,
        forceDrop = false).run(sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE DATAMAP"
}

