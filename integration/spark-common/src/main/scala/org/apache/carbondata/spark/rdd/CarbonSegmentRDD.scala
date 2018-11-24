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

import java.io.IOException
import java.util
import java.util.List

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.block.{SegmentProperties, TableBlockInfo}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory
import org.apache.carbondata.core.scan.model.QueryModelBuilder
import org.apache.carbondata.core.scan.result.RowBatch
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util.{CarbonUtil, DataTypeUtil}
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.CarbonInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.merger.CarbonCompactionUtil

class CarbonInputSplitPartition(
    val idx: Int,
    @transient val carbonInputSplit: CarbonInputSplit)
  extends Partition {
  val split = new SerializableWritable[CarbonInputSplit](carbonInputSplit)

  override val index: Int = idx
}

class CarbonSegmentRawReaderRDD(
    @transient private val sparkSession: SparkSession,
    private val carbonTable: CarbonTable,
    private val segment: Segment,
    var maxSegmentColCardinality: Array[Int] = null,
    var maxSegmentColumnSchemaList: List[ColumnSchema]= new util.ArrayList[ColumnSchema](),
    var totalLength: Long = 0
) extends CarbonRDD[Array[Object]](sparkSession, Nil) {

  private val queryId = System.nanoTime() + ""

  private def getCarbonInputSplits() = {
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val jobConf: JobConf = new JobConf(getConf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job: Job = new Job(jobConf)

    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    val segments = new util.ArrayList[Segment](1)
    segments.add(segment)
    CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, segments)
    job.getConfiguration.set("query.id", queryId)
    val splits = format.getSplits(job)

    if (null != splits && splits.size > 0) {
      splits.asScala
        .map(_.asInstanceOf[CarbonInputSplit])
        .filter { split => FileFormat.COLUMNAR_V3.equals(split.getFileFormat) }
        .toArray
    } else {
      new Array[CarbonInputSplit](0)
    }
  }

  @transient private lazy val _partitions: Array[Partition] = {
    val columnToCardinalityMap = new util.HashMap[java.lang.String, Integer]()
    val carbonInputSplits = getCarbonInputSplits()

    carbonInputSplits.foreach { split =>

      val file = FileFactory.getCarbonFile(split.getPath.toString, getConf)
      totalLength = totalLength + file.getSize
      var dataFileFooter: DataFileFooter = null
      // Check the cardinality of each columns and set the highest.
      try {
        dataFileFooter = CarbonUtil.readMetadataFile(
          CarbonInputSplit.getTableBlockInfo(split))
      } catch {
        case e: IOException =>
          logError("Exception in preparing the data file footer for compaction " + e.getMessage)
          throw e
      }
      // add all the column and cardinality to the map
      CarbonCompactionUtil
        .addColumnCardinalityToMap(columnToCardinalityMap,
          dataFileFooter.getColumnInTable,
          dataFileFooter.getSegmentInfo.getColumnCardinality)
    }
    // update cardinality and column schema list according to master schema
    maxSegmentColCardinality = CarbonCompactionUtil
      .updateColumnSchemaAndGetCardinality(columnToCardinalityMap,
        carbonTable,
        maxSegmentColumnSchemaList)

    (0 until carbonInputSplits.length).map { index =>
      new CarbonInputSplitPartition(index, carbonInputSplits(index))
    }.toArray
  }

  override protected def internalGetPartitions: Array[Partition] = {
    _partitions
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[Array[Object]] = {
    val queryStartTime = System.currentTimeMillis()
    val iter = new Iterator[Array[Object]] {
      val carbonInputSplit = split.asInstanceOf[CarbonInputSplitPartition].split.value

      var mergeStatus = false
      var mergeNumber = ""
      val blockInfo = CarbonInputSplit.getTableBlockInfo(carbonInputSplit)

      // get destination segment properties as sent from driver which is of last segment.
      val segmentProperties = new SegmentProperties(
        maxSegmentColumnSchemaList,
        maxSegmentColCardinality)
      var dataFileMatadata: DataFileFooter = null
      if (null != blockInfo.getDetailInfo()
          && blockInfo.getDetailInfo().getSchemaUpdatedTimeStamp() == 0L) {
        dataFileMatadata = CarbonUtil.readMetadataFile(blockInfo, true);
      } else {
        dataFileMatadata = CarbonUtil.readMetadataFile(blockInfo);
      }

      val restructuredBlockExists: Boolean =
        carbonTable.getTableLastUpdatedTime > dataFileMatadata.getSchemaUpdatedTimeStamp
      DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)

      val builder = new QueryModelBuilder(carbonTable)
        .projectAllColumns()
        .dataConverter(new SparkDataTypeConverterImpl)
        .enableForcedDetailRawQuery()
      val queryModel = builder.build()
      val segmentNo = segment.getSegmentNo

      val blockInfos = new util.ArrayList[TableBlockInfo](1)
      blockInfos.add(blockInfo)
      queryModel.setTableBlockInfos(blockInfos)
      val queryExecutor = QueryExecutorFactory
        .getQueryExecutor(queryModel, FileFactory.getConfiguration)
      val batchIterator = queryExecutor.execute(queryModel).asInstanceOf[CarbonIterator[RowBatch]]
      val resultIterator = new RawResultIterator(batchIterator,
        segmentProperties,
        segmentProperties)

      override def hasNext: Boolean = {
        resultIterator.hasNext
      }

      override def next(): Array[Object] = {
        resultIterator.next()
      }
    }
    iter
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonInputSplitPartition]
    val firstOptionLocation = theSplit.split.value.getLocations.filter(_ != "localhost")
    firstOptionLocation
  }
}