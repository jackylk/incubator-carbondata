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
import org.apache.carbondata.core.indexstore.{BlockletDataMapIndexWrapper, TableBlockIndexUniqueIdentifierWrapper}
import org.apache.carbondata.core.indexstore.blockletindex.{BlockDataMap, BlockletDataMap, BlockletDataMapDistributable, BlockletDataMapFactory}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.hadoop.{CacheClient, DistributableBlockletDataMapLoader}

class SparkBlockletDataMapLoaderJob extends AbstractDataMapJob {

  override def execute(carbonTable: CarbonTable,
    dataMapFormat: FileInputFormat[Void, BlockletDataMapIndexWrapper]): Unit = {
    val dataMapFactory = DataMapStoreManager.getInstance()
      .getDataMapFactoryClass(carbonTable, BlockletDataMapFactory.DATA_MAP_SCHEMA)
    val cacheableDataMap = dataMapFactory.asInstanceOf[CacheableDataMap]
    var startTime = System.currentTimeMillis()
    val dataMapIndexWrappers = new DataMapLoaderRDD(SparkContext.getOrCreate(),
      dataMapFormat.asInstanceOf[DistributableBlockletDataMapLoader]).collect()
    val cacheClient = new CacheClient
    val tableColumnSchema = CarbonUtil
      .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName),
        carbonTable.getMeasureByTableName(carbonTable.getTableName))
    val executorService: ExecutorService = Executors.newFixedThreadPool(3)
    try {
      dataMapIndexWrappers.toList.foreach { dataMapIndexWrapper =>
        executorService
          .submit(new DataMapCacher(cacheableDataMap,
            dataMapIndexWrapper,
            cacheClient,
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
  dataMapIndexWrapper: (TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper),
  cacheClient: CacheClient,
  carbonTable: CarbonTable,
  tableColumnSchema: util.List[ColumnSchema]) extends Callable[Unit] {
  override def call(): Unit = {
    val dataMaps: util.List[BlockDataMap] = dataMapIndexWrapper._2.getDataMaps
    // TODO: Implement dataMaps to add segment properties
    cacheableDataMap.cache(dataMapIndexWrapper._1, dataMapIndexWrapper._2)
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
  extends CarbonRDD[(TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper)](sc,
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
  Iterator[(TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper)] = {
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val distributable = split.asInstanceOf[DataMapLoaderPartition].inputSplit
      .asInstanceOf[BlockletDataMapDistributable]
    val inputSplit = split.asInstanceOf[DataMapLoaderPartition].inputSplit
    val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
    reader.initialize(inputSplit, attemptContext)
    val iter = new Iterator[(TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper)] {

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

      override def next(): (TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper) = {
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
