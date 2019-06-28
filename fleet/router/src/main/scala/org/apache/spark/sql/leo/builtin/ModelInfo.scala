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

package org.apache.spark.sql.leo.builtin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Logical plan for ModelInfo TVF
 */
case class ModelInfo(output: Seq[Attribute], param: ModelInfoParams)
  extends LeafNode with MultiInstanceRelation {
  override def newInstance(): ModelInfo = copy(output = output.map(_.newInstance()))
}

class ModelInfoParams(exprs: Seq[Expression]) {
  val udfName: String = exprs.map(_.asInstanceOf[UnresolvedAttribute]).head.name

  def toSeq: Seq[String] = Seq(udfName)
}

/**
 * Physical plan for ModelInfo TVF
 */
case class ModelInfoExec(
    session: SparkSession,
    udfInfo: ModelInfo) extends LeafExecNode {

  override def output: Seq[Attribute] = udfInfo.output

  override protected def doExecute(): RDD[InternalRow] = null
}
