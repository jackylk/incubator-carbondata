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

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{Callable, Executors, ExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkContext, TaskContext, TaskKilledException}

import org.apache.carbondata.core.datamap.{AbstractDataMapJob, DataMapStoreManager}
import org.apache.carbondata.core.datamap.dev.CacheableDataMap
import org.apache.carbondata.core.indexstore.{BlockletDataMapIndexWrapper, TableBlockIndexUniqueIdentifier, TableBlockIndexUniqueIdentifierWrapper}
import org.apache.carbondata.core.indexstore.blockletindex.{BlockDataMap, BlockletDataMapDistributable, BlockletDataMapFactory}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.{BlockletDataMapDetailsWithSchema, CarbonUtil}
import org.apache.carbondata.hadoop.DistributableBlockletDataMapLoader

class SparkBlockletDataMapLoaderJob extends AbstractDataMapJob {

  override def execute(carbonTable: CarbonTable,
    dataMapFormat: FileInputFormat[Void, BlockletDataMapIndexWrapper]): Unit = {
    val dataMapFactory = DataMapStoreManager.getInstance()
      .getDataMapFactoryClass(carbonTable, BlockletDataMapFactory.DATA_MAP_SCHEMA)
    val cacheableDataMap = dataMapFactory.asInstanceOf[CacheableDataMap]
    var startTime = System.currentTimeMillis()
    val dataMapIndexWrappers = new DataMapLoaderRDD(SparkContext.getOrCreate(),
      dataMapFormat.asInstanceOf[DistributableBlockletDataMapLoader]).collect()
    val tableColumnSchema = CarbonUtil
      .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName),
        carbonTable.getMeasureByTableName(carbonTable.getTableName))
    val executorService: ExecutorService = Executors.newFixedThreadPool(3)
    try {
      dataMapIndexWrappers.toList.foreach { dataMapIndexWrapper =>
        executorService
          .submit(new DataMapCacher(cacheableDataMap,
            dataMapIndexWrapper,
            carbonTable,
            tableColumnSchema))
      }
    } finally {
      executorService.shutdown()
      executorService.awaitTermination(10, TimeUnit.MINUTES)
    }
  }
}

class DataMapCacher(
    cacheableDataMap: CacheableDataMap,
    dataMapIndexWrapper: (TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema),
    carbonTable: CarbonTable,
    tableColumnSchema: util.List[ColumnSchema]) extends Callable[Unit] {
  override def call(): Unit = {
    val dataMaps: util.List[BlockDataMap] = dataMapIndexWrapper._2.getBlockletDataMapIndexWrapper
      .getDataMaps
    val segmentId = dataMapIndexWrapper._1.getSegmentId
    dataMaps.asScala.foreach { dataMap =>
      // add segment properties for each instance
      val segmentPropertiesIndex = dataMapIndexWrapper._2
        .addSegmentProperties(carbonTable, segmentId, dataMap.getSegmentPropertiesIndex)
      dataMap.setSegmentPropertiesIndex(segmentPropertiesIndex)
    }
    // create identifier wrapper object
    val tableBlockIndexUniqueIdentifierWrapper: TableBlockIndexUniqueIdentifierWrapper = new
        TableBlockIndexUniqueIdentifierWrapper(
          dataMapIndexWrapper._1,
          carbonTable)
    // add dataMap to cache
    cacheableDataMap
      .cache(tableBlockIndexUniqueIdentifierWrapper,
        dataMapIndexWrapper._2.getBlockletDataMapIndexWrapper)
  }
}

class DataMapLoaderPartition(rddId: Int, idx: Int, val inputSplit: InputSplit)
  extends Partition {
  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * This RDD is used to load the dataMaps of a segment
 *
 * @param sc
 * @param dataMapFormat
 */
class DataMapLoaderRDD(
  sc: SparkContext,
  dataMapFormat: DistributableBlockletDataMapLoader)
  extends CarbonRDD[(TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema)](sc,
    Nil,
    sc.hadoopConfiguration) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def getPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val splits = dataMapFormat.getSplits(job)
    splits.asScala.zipWithIndex.map(f => new DataMapLoaderPartition(id, f._2, f._1)).toArray
  }

  override def internalCompute(split: Partition, context: TaskContext):
  Iterator[(TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema)] = {
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val distributable = split.asInstanceOf[DataMapLoaderPartition].inputSplit
      .asInstanceOf[BlockletDataMapDistributable]
    val inputSplit = split.asInstanceOf[DataMapLoaderPartition].inputSplit
    val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
    val iter = new Iterator[(TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema)] {
      // in case of success, failure or cancelation clear memory and stop execution
      context.addTaskCompletionListener { context =>
        reader.close()
      }
      reader.initialize(inputSplit, attemptContext)

      private var havePair = false
      private var finished = false


      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        !finished
      }

      override def next(): (TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val value = reader.getCurrentValue
        val key = reader.getCurrentKey
        (key, value)
      }
    }
    iter
  }
}
