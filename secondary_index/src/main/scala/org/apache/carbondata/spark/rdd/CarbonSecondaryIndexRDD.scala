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

import java.util
import java.util.{Collections, List}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.command.SecondaryIndex
import org.apache.spark.sql.execution.command.NodeInfo
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.{Distributable, TableBlockInfo, TaskBlockInfo}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.mutate.UpdateVO
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.util.{CarbonInputFormatUtil, CarbonInputSplitTaskInfo}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CarbonCompactionUtil
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.SecondaryIndexCreationResult
import org.apache.carbondata.spark.spark.secondaryindex.{CarbonSecondaryIndexExecutor, SecondaryIndexQueryResultProcessor, SecondaryIndexUtil}


class CarbonSecondaryIndexRDD[K, V](
    @transient ss: SparkSession,
    result: SecondaryIndexCreationResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    secondaryIndex: SecondaryIndex,
    segmentId: String,
    confExecutorsTemp: String,
    indexCarbonTable: CarbonTable,
    forceAccessSegment: Boolean = false)
  extends CarbonRDD[(K, V)](ss, Nil) {

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  val defaultParallelism = sparkContext.defaultParallelism
  sparkContext.setLocalProperty("spark.scheduler.pool", "DDL")
  sparkContext.setLocalProperty("spark.job.interruptOnCancel", "true")

  var localStoreLocation: String = null
  var columnCardinality: Array[Int] = Array[Int]()
  val carbonStoreLocation = carbonLoadModel.getTablePath
  val databaseName = carbonLoadModel.getDatabaseName
  val factTableName = carbonLoadModel.getTableName
  val tableId = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier
    .getCarbonTableIdentifier.getTableId
  val factToIndexColumnMapping: Array[Int] = SecondaryIndexUtil
    .prepareColumnMappingOfFactToIndexTable(
      databaseName, factTableName, secondaryIndex.indexTableName, false)
  val factToIndexDictColumnMapping: Array[Int] = SecondaryIndexUtil
    .prepareColumnMappingOfFactToIndexTable(
      databaseName, factTableName, secondaryIndex.indexTableName, true)

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      var uniqueStatusId = segmentId + CarbonCommonConstants.UNDERSCORE +
                           theSplit.index
      var secondaryIndexCreationStatus = false
      var exec: CarbonSecondaryIndexExecutor = null
      try {
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        // this property is used to determine whether temp location for carbon is inside
        // container temp dir or is yarn application directory.
        val carbonUseLocalDir = CarbonProperties.getInstance()
          .getProperty("carbon.use.local.dir", "false")
        if ("true".equalsIgnoreCase(carbonUseLocalDir)) {
          val storeLocations = org.apache.carbondata.spark.util.Util
            .getConfiguredLocalDirs(SparkEnv.get.conf)
          if (null != storeLocations && storeLocations.length > 0) {
            localStoreLocation = storeLocations(Random.nextInt(storeLocations.length))
          }
          if (localStoreLocation == null) {
            localStoreLocation = System.getProperty("java.io.tmpdir")
          }
        }
        else {
          localStoreLocation = System.getProperty("java.io.tmpdir")
        }
        localStoreLocation = localStoreLocation + '/' + System.nanoTime() + '_' + theSplit.index
        LOGGER.info("Temp storeLocation taken is " + localStoreLocation)
        val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]
        // sorting the table block info List.
        val splitList = carbonSparkPartition.split.value.getAllSplits
        val tableBlockInfoList = CarbonInputSplit.createBlocks(splitList)
        Collections.sort(tableBlockInfoList)
        val taskAndBlockMapping: TaskBlockInfo =
          SecondaryIndexUtil.createTaskAndBlockMapping(tableBlockInfoList)
        exec = new CarbonSecondaryIndexExecutor(taskAndBlockMapping,
          carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          secondaryIndex.columnNames.asJava,
          new SparkDataTypeConverterImpl
        )
        // fire a query and get the results.
        val queryResultIterators = exec.processTableBlocks()
//        carbonLoadModel.setPartitionId("0")
        carbonLoadModel.setSegmentId(segmentId)
        val tempLocationKey = CarbonDataProcessorUtil
          .getTempStoreLocationKey(carbonLoadModel.getDatabaseName,
            secondaryIndex.indexTableName,
            carbonLoadModel.getSegmentId,
            carbonLoadModel.getTaskNo,
            false,
            false)
        CarbonProperties.getInstance().addProperty(tempLocationKey, localStoreLocation)
        CarbonMetadata.getInstance().addCarbonTable(indexCarbonTable)
        val secondaryIndexQueryResultProcessor: SecondaryIndexQueryResultProcessor = new
            SecondaryIndexQueryResultProcessor(
              carbonLoadModel,
              columnCardinality,
              segmentId,
              secondaryIndex.indexTableName,
              factToIndexColumnMapping,
              factToIndexDictColumnMapping)
        context.addTaskCompletionListener { context =>
          if (null != secondaryIndexQueryResultProcessor) {
            secondaryIndexQueryResultProcessor.close()
          }
        }
        secondaryIndexQueryResultProcessor.processQueryResult(queryResultIterators)
        secondaryIndexCreationStatus = true
      } catch {
        case ex: Throwable =>
          LOGGER.error("Exception occurred in secondary index creation rdd: " + ex)
          throw ex
      } finally {
        if (null != exec) {
          exec.finish()
        }
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueStatusId, secondaryIndexCreationStatus)
      }


    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    theSplit.split.value.getLocations.filter(_ != "localhost")
  }

  override def internalGetPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
      carbonStoreLocation, databaseName, factTableName, tableId)
    val updateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable)
    val jobConf: JobConf = new JobConf(hadoopConf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)
    var defaultParallelism = sparkContext.defaultParallelism
    val result = new java.util.ArrayList[Partition](defaultParallelism)
    var partitionNo = 0
    var columnSize = 0
    var noOfBlocks = 0

    // mapping of the node and block list.
    var nodeBlockMapping: java.util.Map[String, java.util.List[Distributable]] = new
        java.util.HashMap[String, java.util.List[Distributable]]

    val taskInfoList = new java.util.ArrayList[Distributable]
    var carbonInputSplits = mutable.Seq[CarbonInputSplit]()

    var splitsOfLastSegment: List[CarbonInputSplit] = null
    // map for keeping the relation of a task and its blocks.
    val taskIdMapping: java.util.Map[String, java.util.List[CarbonInputSplit]] = new
        java.util.HashMap[String, java.util.List[CarbonInputSplit]]

    // map for keeping the relation of a task and its blocks.
    job.getConfiguration.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, segmentId)
    CarbonInputFormat.setValidateSegmentsToAccess(job.getConfiguration, false)

    val updateDetails: UpdateVO = updateStatusManager.getInvalidTimestampRange(segmentId)

    // get splits
    val splits = format.getSplits(job)

    // keep on assigning till last one is reached.
    if (!splits.isEmpty) {
      splitsOfLastSegment = splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).toList.asJava


      carbonInputSplits ++:= splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

      val columnToCardinalityMap = new util.HashMap[java.lang.String, Integer]()

      carbonInputSplits.foreach(splits => {
        val taskNo = splits.taskId

        val splitList = taskIdMapping.get(taskNo)
        noOfBlocks += 1
        if (null == splitList) {
          val splitTempList = new util.ArrayList[CarbonInputSplit]()
          splitTempList.add(splits)
          taskIdMapping.put(taskNo, splitTempList)
        } else {
          splitList.add(splits)
        }

      }
      )

      taskIdMapping.asScala.foreach(
        entry =>
          taskInfoList
            .add(new CarbonInputSplitTaskInfo(entry._1, entry._2).asInstanceOf[Distributable])
      )

      val nodeBlockMap = CarbonLoaderUtil.nodeBlockMapping(taskInfoList, -1)

      val nodeTaskBlocksMap = new java.util.HashMap[String, java.util.List[NodeInfo]]()

      val confExecutors = confExecutorsTemp.toInt
      val requiredExecutors = if (nodeBlockMap.size > confExecutors) {
        confExecutors
      } else { nodeBlockMap.size() }
      DistributionUtil.ensureExecutors(sparkContext, requiredExecutors, taskInfoList.size)
      logInfo("No.of Executors required=" + requiredExecutors +
              " , spark.executor.instances=" + confExecutors +
              ", no.of.nodes where data present=" + nodeBlockMap.size())
      var nodes = DistributionUtil.getNodeList(sparkContext)
      var maxTimes = 30
      while (nodes.length < requiredExecutors && maxTimes > 0) {
        Thread.sleep(500)
        nodes = DistributionUtil.getNodeList(sparkContext)
        maxTimes = maxTimes - 1
      }
      logInfo(
        "Time taken to wait for executor allocation is =" + ((30 - maxTimes) * 500) + "millis")
      defaultParallelism = sparkContext.defaultParallelism

      // Create Spark Partition for each task and assign blocks
      nodeBlockMap.asScala.foreach { case (nodeName, splitList) =>
        val taskSplitList = new java.util.ArrayList[NodeInfo](0)
        nodeTaskBlocksMap.put(nodeName, taskSplitList)
        var blockletCount = 0
        splitList.asScala.foreach { splitInfo =>
          val splitsPerNode = splitInfo.asInstanceOf[CarbonInputSplitTaskInfo]
          blockletCount = blockletCount + splitsPerNode.getCarbonInputSplitList.size()
          taskSplitList.add(
            NodeInfo(splitsPerNode.getTaskId, splitsPerNode.getCarbonInputSplitList.size()))

          if (blockletCount != 0) {
            val multiBlockSplit = new CarbonMultiBlockSplit(
              splitInfo.asInstanceOf[CarbonInputSplitTaskInfo].getCarbonInputSplitList,
              Array(nodeName))
            result.add(new CarbonSparkPartition(id, partitionNo, multiBlockSplit))
            partitionNo += 1
          }
        }
      }

      // print the node info along with task and number of blocks for the task.
      nodeTaskBlocksMap.asScala.foreach((entry: (String, List[NodeInfo])) => {
        logInfo(s"for the node ${ entry._1 }")
        for (elem <- entry._2.asScala) {
          logInfo("Task ID is " + elem.TaskId + "no. of blocks is " + elem.noOfBlocks)
        }
      })

      val noOfNodes = nodes.length
      val noOfTasks = result.size
      logInfo(s"Identified  no.of.Blocks: $noOfBlocks," +
              s"parallelism: $defaultParallelism , no.of.nodes: $noOfNodes, no.of.tasks: " +
              s"$noOfTasks")
      logInfo("Time taken to identify Blocks to scan : " + (System.currentTimeMillis() - startTime))
      for (j <- 0 until result.size) {
        val multiBlockSplit = result.get(j).asInstanceOf[CarbonSparkPartition].split.value
        val splitList = multiBlockSplit.getAllSplits
        val tableBlocks: util.List[TableBlockInfo] = CarbonInputSplit.createBlocks(splitList)
        val tableBlocksSize: Int = tableBlocks.size
        if (tableBlocksSize > 0) {
          // read the footer and get column cardinality which will be same for all tasks in a
          // segment
          val dataFileFooter: DataFileFooter = SecondaryIndexUtil
            .readFileFooter(tableBlocks.get(tableBlocks.size() - 1))
          CarbonCompactionUtil
            .addColumnCardinalityToMap(columnToCardinalityMap,
              dataFileFooter.getColumnInTable,
              dataFileFooter.getSegmentInfo.getColumnCardinality)
        }
        logInfo(s"Node: ${ multiBlockSplit.getLocations.mkString(",") }, No.Of Blocks: " +
                s"${ CarbonInputSplit.createBlocks(splitList).size }")
      }
      val updatedMaxSegmentColumnList = new util.ArrayList[ColumnSchema]()
      // update cardinality and column schema list according to master schema
      val factTableCardinality: Array[Int] = CarbonCompactionUtil
        .updateColumnSchemaAndGetCardinality(columnToCardinalityMap,
          carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          updatedMaxSegmentColumnList)
      // prepare the cardinality based on the SI table schema
      columnCardinality = SecondaryIndexUtil
        .prepareColumnCardinalityForIndexTable(factTableCardinality,
          databaseName,
          factTableName,
          indexCarbonTable.getTableName)
      result.toArray(new Array[Partition](result.size))
    } else {
      new Array[Partition](0)
    }
  }
}
