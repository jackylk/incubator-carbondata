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
package org.apache.spark.sql.execution.joins

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport}

/**
  * trait for handling Broadcast filter join related operations
  */
trait CarbonBroadCastFilterPushJoin extends BinaryExecNode with HashJoin with CodegenSupport {

  private var copyResult: Boolean = false

  override def needCopyResult: Boolean = {
    copyResult
  }

  protected def setCopyResult(ctx: CodegenContext, needCopyResult: Boolean): Unit = {
    copyResult = needCopyResult
  }

  protected def addMutableStateToContext(ctx: CodegenContext,
    className: String,
    broadcast: String): String = {
    ctx.addMutableState(className, "relation",
      x =>
        s"""
           | $x = (($className) $broadcast.value()).asReadOnlyCopy();
           | incPeakExecutionMemory($x.estimatedSize());
       """.stripMargin)
  }

  protected def performJoinOperation(sparkContext: SparkContext,
    streamedPlanOutput: RDD[InternalRow],
    broadcastRelation: Broadcast[HashedRelation],
    numOutputRows: SQLMetric): RDD[InternalRow] = {
    val averageMetric = SQLMetrics.createAverageMetric(sparkContext, "join")
    streamedPlanOutput.mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashedRelation.estimatedSize)
      join(streamedIter, hashedRelation, numOutputRows, averageMetric)
    }
  }

}
