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

package org.apache.carbondata.spark.rdd

import java.util
import java.util.{List, UUID}

import scala.reflect.classTag
import scala.util.control.Breaks._

import org.apache.spark.{RangePartitioner, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionResultSortProcessor, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.util.CommonUtil

object CarbonOptimizeTableUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def repartitionSegment(
      sparkSession: SparkSession,
      table: CarbonTable,
      segment: Segment,
      column: CarbonColumn): Unit = {
    var isUpdateTableStatusRequired = false
    val carbonLoadModel = CarbonDataRDDFactory.prepareCarbonLoadModel(table)
    openSegment(carbonLoadModel)
    isUpdateTableStatusRequired = true
    val operationContext: OperationContext = new OperationContext
    val uuid = if (table.isChildDataMap) {
      Option(operationContext.getProperty("uuid")).getOrElse("").toString
    } else if (table.hasAggregationDataMap) {
      UUID.randomUUID().toString
    } else {
      ""
    }
    operationContext.setProperty("uuid", uuid)
    var numPartitions: Int = 0
    var statusList: Array[Boolean] = Array.empty
    try {
      // read raw data of one segment
      val segmentRDD = new CarbonSegmentRawReaderRDD(sparkSession, table, segment)
      segmentRDD.getPartitions
      numPartitions = calculateNumPartitions(table, segmentRDD.totalLength)
      val rowKey = rowKeyOfColumn(
        table,
        column,
        segmentRDD.maxSegmentColCardinality,
        segmentRDD.maxSegmentColumnSchemaList)
      val pairRDD = segmentRDD.keyBy(rowKey.key(_))
      val columnOrdering = orderingOfColumn(column)
      statusList = pairRDD
        .partitionBy(
          new RangePartitioner(numPartitions, pairRDD)(columnOrdering, classTag[Object]))
        .map(_._2)
        .mapPartitionsWithIndex { (index, iterator) =>
          new Iterator[Boolean] {
            val status = localSortAndWriteDataFile(
              carbonLoadModel,
              iterator,
              segmentRDD.maxSegmentColCardinality,
              segmentRDD.maxSegmentColumnSchemaList,
              index)

            var finished = false

            override def hasNext: Boolean = {
              !finished
            }

            override def next(): Boolean = {
              finished = true
              status
            }
          }
        }.collect()

    } finally {
      // check statusList
      val finalStatus = statusList.exists(!_)
      if (finalStatus) {
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        val msg = s"Failed to repartition segment ${ segment.getSegmentNo } on " +
                  s"${ table.getTableName }.${ column.getColName } "
        LOGGER.error(msg)
        throw new CarbonDataLoadingException(msg)
      } else {
        finishSegment(sparkSession, table, segment, carbonLoadModel, operationContext)
        LOGGER.info(
          s"Success to repartition segment ${ segment.getSegmentNo } to " +
          s"${ numPartitions } partitions on ${ table.getTableName }.${ column.getColName } ")
      }
    }

  }

  private def openSegment(carbonLoadModel: CarbonLoadModel) = {
    // update tablestatus file
    CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, false)
  }

  private def finishSegment(sparkSession: SparkSession,
      table: CarbonTable,
      segment: Segment,
      carbonLoadModel: CarbonLoadModel,
      operationContext: OperationContext) = {
    val segmentFileName = SegmentFileStore.writeSegmentFile(
      table,
      carbonLoadModel.getTaskNo,
      carbonLoadModel.getFactTimeStamp.toString)
    val loadsToMerge = new util.ArrayList[LoadMetadataDetails]()
    loadsToMerge.add(segment.getLoadMetadataDetails)
    CarbonDataMergerUtil.updateLoadMetadataWithOptimizeStatus(
      loadsToMerge,
      table.getMetadataPath,
      carbonLoadModel.getSegmentId,
      carbonLoadModel,
      segmentFileName)
    val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
      new LoadTablePostExecutionEvent(
        table.getCarbonTableIdentifier,
        carbonLoadModel)
    OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
    val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(table)
    if (null != tableDataMaps) {
      val buildDataMapPostExecutionEvent = new BuildDataMapPostExecutionEvent(
        sparkSession, table.getAbsoluteTableIdentifier,
        null, Seq(carbonLoadModel.getSegmentId), true)
      OperationListenerBus.getInstance()
        .fireEvent(buildDataMapPostExecutionEvent, operationContext)
    }
  }

  def calculateNumPartitions(table: CarbonTable, totalLength: Long): Int = {
    val tableBlockSize = 1024L * 1024 * table.getBlockSizeInMB
    val tableBlockletSize = 1024L * 1024 * table.getBlockletSizeInMB
    val fileSize = Math.max(tableBlockletSize, tableBlockSize - tableBlockletSize)
    (totalLength / fileSize + 1).toInt
  }

  def localSortAndWriteDataFile(
      carbonLoadModel: CarbonLoadModel,
      iterator: Iterator[Array[Object]],
      maxSegmentColCardinality: Array[Int],
      maxSegmentColumnSchemaList: List[ColumnSchema],
      taskIndex: Int): Boolean = {
    carbonLoadModel.setTaskNo(taskIndex + "")
    CommonUtil.setTempStoreLocation(taskIndex, carbonLoadModel, true, false)
    val segmentProperties = new SegmentProperties(
      maxSegmentColumnSchemaList,
      maxSegmentColCardinality)
    val processor = new CompactionResultSortProcessor(
      carbonLoadModel,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
      segmentProperties,
      CompactionType.NONE,
      carbonLoadModel.getTableName,
      null)
    // add task completion listener to release resource
    TaskContext.get().addTaskCompletionListener(_ => close(carbonLoadModel, processor))
    val iteratorList = new util.ArrayList[RawResultIterator](1)
    iteratorList.add(new RawResultIterator(null, null, null) {
      override def next(): Array[AnyRef] = iterator.next()
      override def hasNext: Boolean = iterator.hasNext
    })
    try {
      processor.execute(iteratorList)
    } catch {
      case e: Exception =>
        LOGGER.error("localSortAndWriteDataFile Failed ", e)
        throw e
    }
  }

  private def close(carbonLoadModel: CarbonLoadModel,
      processor: CompactionResultSortProcessor): Unit = {
    try {
      LOGGER.info("Deleting local folder store location")
      val isCompactionFlow = true
      TableProcessingOperations
        .deleteLocalDataLoadFolderLocation(carbonLoadModel, isCompactionFlow, false)
    } catch {
      case e: Exception =>
        LOGGER.error(e)
    }
    // clean up the resources for processor
    if (null != processor) {
      LOGGER.info("Closing compaction processor instance to clean up loading resources")
      processor.close()
    }
  }

  private def orderingOfColumn(column: CarbonColumn): Ordering[Object] = {
    if (column.isDimension) {
      new DimensionOrdering()
    } else {
      new PrimtiveOrdering(column.getDataType)
    }
  }

  private def rowKeyOfColumn(
      table: CarbonTable,
      column: CarbonColumn,
      maxSegmentColCardinality: Array[Int],
      maxSegmentColumnSchemaList: List[ColumnSchema]
  ): RowKey = {
    if (column.isDimension) {
      val dimensions = table.getDimensions
      val dimension = column.asInstanceOf[CarbonDimension]
      if (dimension.isDirectDictionaryEncoding
          || dimension.isGlobalDictionaryEncoding) {
        getRowKeyOfDictDimension(
          dimensions,
          dimension,
          maxSegmentColCardinality,
          maxSegmentColumnSchemaList)
      } else {
        getRowKeyOfNoDictDimension(dimensions, dimension)
      }
    } else {
      new PrimtiveRowKey(column.getOrdinal + 1)
    }
  }

  private def getRowKeyOfDictDimension(
      dimensions: List[CarbonDimension],
      dimension: CarbonDimension,
      maxSegmentColCardinality: Array[Int],
      maxSegmentColumnSchemaList: List[ColumnSchema]
  ): RowKey = {
    val segmentProperties = new SegmentProperties(
      maxSegmentColumnSchemaList,
      maxSegmentColCardinality)
    val columnValueSizes = segmentProperties.getEachDimColumnValueSize
    var columnValueSize: Int = 0
    var offset = 0
    breakable {
      (0 to dimensions.size()).foreach { index =>
        if ((dimensions.get(index).isDirectDictionaryEncoding) ||
            (dimensions.get(index).isGlobalDictionaryEncoding)) {
          columnValueSize = columnValueSizes(index)
          if (dimension.getColName.equals(dimensions.get(index).getColName)) {
            break
          }
          if (columnValueSize > 0) {
            offset = offset + columnValueSize
          }
        }
      }
    }
    new DictRowKey(offset, columnValueSize)
  }

  private def getRowKeyOfNoDictDimension(
      dimensions: List[CarbonDimension],
      dimension: CarbonDimension
  ): RowKey = {
    var noDictIndex = -1
    breakable {
      (0 to dimensions.size()).foreach { index =>
        if ((!dimensions.get(index).isDirectDictionaryEncoding) &&
            (!dimensions.get(index).isGlobalDictionaryEncoding)) {
          noDictIndex = noDictIndex + 1
          if (dimension.getColName.equals(dimensions.get(index).getColName)) {
            break
          }
        }
      }
    }
    new NoDictRowKey(noDictIndex)
  }
}


class PrimtiveOrdering(dataType: DataType) extends Ordering[Object] {
  val comparator = org.apache.carbondata.core.util.comparator.Comparator
    .getComparator(dataType)

  override def compare(x: Object, y: Object): Int = {
    comparator.compare(x, y)
  }
}

class DimensionOrdering() extends Ordering[Object] {
  override def compare(x: Object, y: Object): Int = {
    UnsafeComparer.INSTANCE.compareTo(x.asInstanceOf[Array[Byte]], y.asInstanceOf[Array[Byte]])
  }
}

trait RowKey extends java.io.Serializable {
  def key(row: Array[Object]): Object
}

class PrimtiveRowKey(index: Int) extends RowKey {
  override def key(row: Array[Object]): Object = {
    row(index)
  }
}

class NoDictRowKey(index: Int) extends RowKey {
  override def key(row: Array[Object]): Object = {
    row(0).asInstanceOf[ByteArrayWrapper].getNoDictionaryKeyByIndex(index)
  }
}

class DictRowKey(offset: Int, length: Int) extends RowKey {
  override def key(row: Array[Object]): Object = {
    val bytes = new Array[Byte](length)
    System.arraycopy(
      row(0).asInstanceOf[ByteArrayWrapper].getDictionaryKey,
      offset,
      bytes,
      0,
      length)
    bytes
  }
}

