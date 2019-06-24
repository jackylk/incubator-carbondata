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

package org.apache.spark.sql.execution.command.vector

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkContext, TaskContext, TaskKilledException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.{BucketFields, Field, TableNewProcessor}
import org.apache.spark.sql.execution.datasources.json.JsonInferSchema
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.spark.rdd.CarbonSparkPartition
import org.apache.carbondata.vector.VectorTableInputFormat
import org.apache.carbondata.vector.column.{VectorColumnReader, VectorColumnWriter}

/**
 * Utility functions for INSERT COLUMN execution
 */
object InsertColumnsHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def insertColumnsForVectorTable(
      sparkSession: SparkSession,
      table: CarbonTable,
      field: Field,
      input: DataFrame,
      hadoopConf: Configuration): Seq[ColumnSchema] = {
    val plan = input.logicalPlan
    validateInput(sparkSession, table, plan, true)
    val optimizedPlan = optimizePlan(plan)
    validateInput(sparkSession, table, optimizedPlan, false)
    val newInput = SparkSQLUtil.ofRows(sparkSession, optimizedPlan)
    if (needInferSchema(field, optimizedPlan)) {
      inferSchemaAndInsertColumn(
        sparkSession,
        table,
        field,
        newInput,
        hadoopConf)
    } else {
      val finalField =
        if (field.dataType.isEmpty) {
          updateField(
            field,
            optimizedPlan.asInstanceOf[Project].projectList(0).dataType)
        } else {
          field
        }
      insertColumnsJob(
        sparkSession,
        table,
        finalField,
        newInput,
        hadoopConf)._2
    }
  }

  def updateField(field: Field, dataType: DataType): Field = {
    val schema = Seq(StructField(field.column, dataType))
    val parser = new CarbonSpark2SqlParser()
    val finalFields = parser.getFields(schema)
    finalFields(0)
  }

  def inferSchemaAndInsertColumn(
      sparkSession: SparkSession,
      table: CarbonTable,
      field: Field,
      input: DataFrame,
      hadoopConf: Configuration
  ): Seq[ColumnSchema] = {
    val fieldName = UUID.randomUUID().toString
    val virtualField = new Field(fieldName, Option("string"), Option(fieldName), None)
    // insert a virtual column
    val (column, _) = insertColumnsJob(
      sparkSession,
      table,
      virtualField,
      input,
      hadoopConf)
    // infer schema
    val scanRDD = new ColumnScanRDD(
      table.getTableInfo,
      column,
      getPartitions(table, hadoopConf).toArray,
      hadoopConf,
      sparkSession.sparkContext
    )
    val dataType = inferSchema(sparkSession, scanRDD)
    // insert json object into table
    val (_, finalColumnSchemas) = insertJsonJob(
      sparkSession,
      table,
      scanRDD,
      updateField(field, dataType),
      dataType,
      hadoopConf)
    deleteTempColumnFiles(table, column, hadoopConf)
    finalColumnSchemas
  }

  def insertJsonJob(sparkSession: SparkSession,
      table: CarbonTable,
      scanRDD: ColumnScanRDD,
      field: Field,
      dataType: DataType,
      hadoopConf: Configuration
  ): (CarbonColumn, Seq[ColumnSchema]) = {
    val (finalColumn, finalColumnSchemas) = generateVirtualColumn(field)
    // convert json string to struct
    val tableInfo = table.getTableInfo
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    import org.apache.spark.sql.functions.from_json
    SparkSQLUtil
      .internalCreateDataFrame(
        sparkSession,
        scanRDD,
        StructType(Seq(StructField("col1", StringType))))
      .select(from_json(Column("col1"), dataType, new util.HashMap[String, String]()))
      .rdd
      .mapPartitions { iterator =>
        val segmentNo = VectorTableInputFormat.getSegmentNo()
        if (segmentNo == null) {
          // no need retry task
          throw new RuntimeException("Not supported this query")
        }
        writeColumnsTask(
          tableInfo,
          finalColumn,
          segmentNo,
          iterator,
          serializedConf.value
        )
      }.collect()
    (finalColumn, finalColumnSchemas)
  }

  def inferSchema(sparkSession: SparkSession, scanRDD: ColumnScanRDD): DataType = {
    val parsedOptions = new JSONOptions(
      Map.empty[String, String],
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    JsonInferSchema.infer(
        scanRDD.map(_.getUTF8String(0)),
        parsedOptions,
        CreateJacksonParser.utf8String)
  }

  def getPartitions(
      table: CarbonTable,
      hadoopConf: Configuration
  ): Seq[Partition] = {
    VectorTableInputFormat
      .getSplit(table, hadoopConf)
      .asScala
      .zipWithIndex
      .map { case (split, index) =>
        new CarbonSparkPartition(
          0,
          index,
          new CarbonMultiBlockSplit(split.asInstanceOf[CarbonInputSplit])
        ).asInstanceOf[Partition]
      }
  }

  def insertColumnsJob(
      sparkSession: SparkSession,
      table: CarbonTable,
      field: Field,
      input: DataFrame,
      hadoopConf: Configuration
  ): (CarbonColumn, Seq[ColumnSchema]) = {
    val (column, columnSchemas) = generateVirtualColumn(field)
    // write columns data for each segment
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    var hasException = false
    try {
      val tableInfo = table.getTableInfo
      input
        .rdd
        .mapPartitions { iterator =>
          val segmentNo = VectorTableInputFormat.getSegmentNo()
          if (segmentNo == null) {
            // no need retry task
            throw new RuntimeException("Not supported this query")
          }
          writeColumnsTask(
            tableInfo,
            column,
            segmentNo,
            iterator,
            serializedConf.value
          )
        }.collect()
      (column, columnSchemas)
    } catch {
      case e =>
        hasException = true
        throw e
    } finally {
      if (hasException) {
        deleteTempColumnFiles(table, column, hadoopConf)
      }
    }
  }

  def needInferSchema(field: Field, inputPlan: LogicalPlan): Boolean = {
    val dataType = inputPlan.asInstanceOf[Project].projectList(0).dataType.simpleString
    field.dataType match {
      case None =>
        false
      case Some(fieldType) =>
        if (fieldType == "json" && dataType != "string") {
          throw new AnalysisException(
            "the query data type should be string when the column data type is json")
        }
        dataType == "string" &&
        field.children.isEmpty &&
        (fieldType == "json")
    }
  }

  /**
   * generate a virtual column for insert columns function
   * it is not in the table schema now.
   * @return CarbonDimension or CarbonMeasure
   * @return Seq[ColumnSchema]
   */
  def generateVirtualColumn(field: Field): (CarbonColumn, Seq[ColumnSchema]) = {
    // TODO need to infer data type for abstract complex data type
    val parser = new CarbonSpark2SqlParser()
    val virtualTableModel = parser.prepareTableModel(
      true,
      Option("default"),
      "default",
      Seq(field),
      Seq.empty,
      scala.collection.mutable.Map.empty[String, String],
      Option.empty[BucketFields],
      false,
      false,
      None)
    val virtualTableInfo = TableNewProcessor(virtualTableModel)
    val virtualTable = CarbonTable.buildFromTableInfo(virtualTableInfo)
    val virtualColumn =
      virtualTable
        .getCreateOrderColumn(virtualTable.getTableName)
        .get(0)
    val virtualSchemas =
      virtualTable
        .getTableInfo
        .getFactTable
        .getListOfColumns
        .asScala
        .filter(!_.isInvisible)
    (virtualColumn, virtualSchemas)
  }

  /**
   * write columns into the table for the segment
   * now only support writing one column at once
   * @param tableInfo
   * @param column
   * @param segmentNo
   * @param iterator
   * @param hadoopConf
   * @return
   */
  def writeColumnsTask(
      tableInfo: TableInfo,
      column: CarbonColumn,
      segmentNo: String,
      iterator: Iterator[Row],
      hadoopConf: Configuration
  ): Iterator[Row] = {
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    val writer = new VectorColumnWriter(table, column, segmentNo, hadoopConf)
    try {
      iterator.foreach { value =>
        writer.write(value.get(0))
      }
    } finally {
      writer.close()
    }
    Iterator.empty
  }

  /**
   * the sql syntax of the supported query:
   * select
   * from <table_name>
   * where ...
   *
   * not support: join, group by, aggregate and window function.
   */
  def validateInput(
      sparkSession: SparkSession,
      table: CarbonTable,
      inputPlan: LogicalPlan,
      hasFilter: Boolean): Unit = {

    def matchRelation(plan: LogicalPlan): Unit = {
      plan match {
        case SubqueryAlias(_, child) =>
          child match {
            case LogicalRelation(relation, _, _, _) =>
              if (relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
                val tableId = relation
                  .asInstanceOf[CarbonDatasourceHadoopRelation]
                  .carbonTable
                  .getTableId
                if (!table.getTableId.equalsIgnoreCase(tableId)) {
                  throw new AnalysisException(
                    "The query table should be same with the target table")
                }
              } else {
                throw new AnalysisException("Query is not supported on this table")
              }
            case _ =>
              throw new AnalysisException("Query is not supported")
          }
        case _ =>
          throw new AnalysisException("Query is not supported")
      }
    }

    inputPlan match {
      case Project(objectList, child) =>
        if (objectList.isEmpty || objectList.size != 1) {
          throw new AnalysisException("Only support insert only one column now")
        }
        child match {
          case SubqueryAlias(_, _) =>
            matchRelation(child)
          case Filter(_, child) =>
            if (hasFilter) {
              matchRelation(child)
            } else {
              throw new AnalysisException("Query is not supported")
            }
          case _ =>
            throw new AnalysisException("Query is not supported")
        }
      case _ =>
        throw new AnalysisException("Query is not supported")
    }
  }

  /**
   * if the plan contain filter, move it to the projection
   * @param plan
   * @return the optimized plan
   */
  def optimizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case p@Project(objectList, child) =>
        if (child.isInstanceOf[Filter]) {
          val alias = objectList(0).asInstanceOf[Alias]
          val filter = child.asInstanceOf[Filter]
          val cw = CaseWhen(Seq((filter.condition, alias.child)), None)
          val newObjectList =
            Seq(Alias(cw, alias.name)(alias.exprId, alias.qualifier, alias.explicitMetadata))
          Project(newObjectList, filter.child)
        } else {
          p
        }
      case plan => plan
    }
  }

  def deleteTempColumnFiles(table: CarbonTable,
      column: CarbonColumn,
      hadoopConf: Configuration): Unit = {
    // TODO need to clean temp column files
  }
}

class ColumnScanRDD(
    tableInfo: TableInfo,
    column: CarbonColumn,
    partitions: Array[Partition],
    hadoopConf: Configuration,
    sc: SparkContext) extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    new Iterator[InternalRow] {
      private var havePair = false
      private var finished = false
      private var first = true

      val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
      VectorTableInputFormat.setSegmentNo(
        inputSplit.asInstanceOf[CarbonMultiBlockSplit].getAllSplits.get(0).getSegmentId
      )
      val table = CarbonTable.buildFromTableInfo(tableInfo)
      var reader = new VectorColumnReader(table, column, getConf, false)
      val jobTrackerId: String = {
        val formatter = new SimpleDateFormat("yyyyMMddHHmm")
        formatter.format(new Date())
      }
      val taskId = split.index
      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val attemptContext = new TaskAttemptContextImpl(getConf, attemptId)
      val closeReader = (_: TaskContext) => {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
          }
          reader = null
        }
      }

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (first) {
          first = false
          context.addTaskCompletionListener(closeReader)
          // initialize the reader
          reader.initialize(inputSplit, attemptContext)
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        if (finished) {
          closeReader.apply(context)
        }
        !finished
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val value = reader.getCurrentValue
        value.asInstanceOf[InternalRow]
      }
    }
  }

  val config = SparkSQLUtil.broadCastHadoopConf(sparkContext, hadoopConf)

  def getConf: Configuration = {
    config.value.value
  }
}

