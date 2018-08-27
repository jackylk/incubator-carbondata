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
import org.apache.spark.{Partition, TaskContext, TaskKilledException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.core.datamap.{AbstractDataMapJob, DataMapStoreManager}
import org.apache.carbondata.core.datamap.dev.CacheableDataMap
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.indexstore.{BlockletDataMapIndexWrapper, TableBlockIndexUniqueIdentifier, TableBlockIndexUniqueIdentifierWrapper}
import org.apache.carbondata.core.indexstore.blockletindex.{BlockDataMap, BlockletDataMapDistributable}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{BlockletDataMapDetailsWithSchema, CarbonUtil}
import org.apache.carbondata.hadoop.DistributableBlockletDataMapLoader

// wrapper class for columnCardinality
class ColumnCardinality(val cardinality: Array[Int]) {
  def canEqual(other: Any): Boolean = {
    if (other.asInstanceOf[ColumnCardinality].cardinality.length == cardinality.length) {
      other.asInstanceOf[ColumnCardinality].cardinality.sameElements(cardinality)
    } else {
      false
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: ColumnCardinality => that canEqual this
      case _ => false
    }
  }

  override def hashCode(): Int = {
    cardinality.toSeq.hashCode
  }
}

class SparkBlockletDataMapLoaderJob extends AbstractDataMapJob {

  override def execute(carbonTable: CarbonTable,
    dataMapFormat: FileInputFormat[Void, BlockletDataMapIndexWrapper]): Unit = {
    val dataMapFactory = DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable)
      .getDataMapFactory
    val cacheableDataMap = dataMapFactory.asInstanceOf[CacheableDataMap]
    val dataMapIndexWrappers = new DataMapLoaderRDD(SparkSQLUtil.getSparkSession,
      dataMapFormat.asInstanceOf[DistributableBlockletDataMapLoader]).collect()
    // add segmentProperties in single thread if carbon table schema is not modified
    if (!carbonTable.getTableInfo.isSchemaModified) {
      addSegmentProperties(carbonTable, dataMapIndexWrappers)
    }
    val executorService: ExecutorService = Executors.newFixedThreadPool(3)
    try {
      dataMapIndexWrappers.toList.foreach { dataMapIndexWrapper =>
        executorService
          .submit(new DataMapCacher(cacheableDataMap,
            dataMapIndexWrapper,
            carbonTable))
      }
    } finally {
      executorService.shutdown()
      executorService.awaitTermination(10, TimeUnit.MINUTES)
    }
  }

  private def addSegmentProperties(carbonTable: CarbonTable,
      dataMapIndexWrappers: Array[(TableBlockIndexUniqueIdentifier,
        BlockletDataMapDetailsWithSchema)]): Unit = {
    // unique columnCardinality to each task output map
    val uniqueColumnCardinalityToWrapperList = scala.collection.mutable
      .Map[ColumnCardinality, scala.collection.mutable.ArrayBuffer[(TableBlockIndexUniqueIdentifier,
      BlockletDataMapDetailsWithSchema)]]()
    dataMapIndexWrappers.foreach { wrapper =>
      val columnCardinality = new ColumnCardinality(wrapper._2.getColumnCardinality)
      uniqueColumnCardinalityToWrapperList.get(columnCardinality) match {
        case Some(value) =>
          uniqueColumnCardinalityToWrapperList.get(columnCardinality).get += wrapper
        case None =>
          val dataMapWrapperList = scala.collection.mutable.ArrayBuffer
            .empty[(TableBlockIndexUniqueIdentifier,
            BlockletDataMapDetailsWithSchema)]
          dataMapWrapperList += wrapper
          uniqueColumnCardinalityToWrapperList.put(columnCardinality, dataMapWrapperList)
      }
    }
    // use the carbon table schema only as this flow is called when schema is not modified
    val tableColumnSchema = CarbonUtil
      .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getTableName),
        carbonTable.getMeasureByTableName(carbonTable.getTableName))
    // add segmentProperties in the segmentPropertyCache
    uniqueColumnCardinalityToWrapperList.foreach { entry =>
      val segmentId = entry._2(0)._1.getSegmentId
      val newSegmentPropertyIndex = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable, tableColumnSchema, entry._1.cardinality, segmentId)
      entry._2.foreach { dataMapWrapper =>
        // add all the segmentId's for given segmentPropertyIndex
        SegmentPropertiesAndSchemaHolder.getInstance()
          .addSegmentId(newSegmentPropertyIndex, dataMapWrapper._1.getSegmentId)
        // update all dataMaps with new segmentPropertyIndex
        dataMapWrapper._2.getBlockletDataMapIndexWrapper.getDataMaps.asScala.foreach { dataMap =>
          dataMap.setSegmentPropertiesIndex(newSegmentPropertyIndex)
        }
      }
    }
  }
}

class DataMapCacher(
    cacheableDataMap: CacheableDataMap,
    dataMapIndexWrapper: (TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema),
    carbonTable: CarbonTable) extends Callable[Unit] {
  override def call(): Unit = {
    // if schema is modified then populate the segmentProperties cache
    if (carbonTable.getTableInfo.isSchemaModified) {
      val dataMaps: util.List[BlockDataMap] = dataMapIndexWrapper._2.getBlockletDataMapIndexWrapper
        .getDataMaps
      val newSegmentPropertyIndex = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable,
          dataMapIndexWrapper._2.getColumnSchemaList,
          dataMapIndexWrapper._2.getColumnCardinality,
          dataMapIndexWrapper._1.getSegmentId)
      // update all dataMaps with new segmentPropertyIndex
      dataMaps.asScala.foreach { dataMap =>
        dataMap.setSegmentPropertiesIndex(newSegmentPropertyIndex)
      }
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
 * @param ss
 * @param dataMapFormat
 */
class DataMapLoaderRDD(
  @transient ss: SparkSession,
  dataMapFormat: DistributableBlockletDataMapLoader)
  extends CarbonRDD[(TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema)](ss, Nil) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val splits = dataMapFormat.getSplits(job)
    splits.asScala.zipWithIndex.map(f => new DataMapLoaderPartition(id, f._2, f._1)).toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataMapLoaderPartition].inputSplit.getLocations.filter(_ != "localhost")
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
