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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.vector.VectorTableInputFormat
import org.apache.carbondata.vector.table.VectorColumnWriter

/**
 * Utility functions for INSERT COLUMN execution
 */
object InsertColumnsHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def insertColumnsForVectorTable(
      sparkSession: SparkSession,
      table: CarbonTable,
      column: CarbonColumn,
      input: DataFrame,
      hadoopConf: Configuration): Unit = {
    validateInput(sparkSession, table, input.logicalPlan, true)
    val (hasOptimized, optimizedPlan) = optimizePlan(input.logicalPlan)
    validateInput(sparkSession, table, optimizedPlan, false)
    // write columns data for each segment
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    val optimizedInput = if (hasOptimized) {
      SparkSQLUtil.ofRows(sparkSession, optimizedPlan)
    } else {
      input
    }
    var hasException = false
    try {
      optimizedInput
        .rdd
        .mapPartitions { iterator =>
          val segmentNo = VectorTableInputFormat.getSegmentNo()
          if (segmentNo == null) {
            // no need retry task
            throw new RuntimeException("Not supported this query")
          }
          writeColumns(
            table,
            column,
            segmentNo,
            iterator,
            serializedConf.value
          )
        }.collect()
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

  /**
   * write columns into the table for the segment
   * now only support writing one column at once
   * @param table
   * @param column
   * @param segmentNo
   * @param iterator
   * @param hadoopConf
   * @return
   */
  def writeColumns(
      table: CarbonTable,
      column: CarbonColumn,
      segmentNo: String,
      iterator: Iterator[Row],
      hadoopConf: Configuration
  ): Iterator[Row] = {
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
  private def validateInput(
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
   * @return true if the plan is optimized, false if else
   * @return the optimized plan
   */
  def optimizePlan(plan: LogicalPlan): (Boolean, LogicalPlan) = {
    var optimized = false
    val optimizedPlan = plan transform {
      case p@Project(objectList, child) =>
        if (child.isInstanceOf[Filter]) {
          val alias = objectList(0).asInstanceOf[Alias]
          val filter = child.asInstanceOf[Filter]
          val cw = CaseWhen(Seq((filter.condition, alias.child)), None)
          val newObjectList =
            Seq(Alias(cw, alias.name)(alias.exprId, alias.qualifier, alias.explicitMetadata))
          optimized = true
          Project(newObjectList, filter.child)
        } else {
          p
        }
      case plan => plan
    }
    (optimized, optimizedPlan)
  }

  def deleteTempColumnFiles(table: CarbonTable,
      column: CarbonColumn,
      hadoopConf: Configuration): Unit = {
    // TODO need to clean temp column files
  }
}


