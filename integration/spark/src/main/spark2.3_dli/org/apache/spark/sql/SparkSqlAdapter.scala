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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv
import org.apache.spark.sql.internal.{SessionState, SparkSessionListener}
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.core.util.ThreadLocalSessionInfo

object SparkSqlAdapter {

  def initSparkSQL(): Unit = {
    SparkSQLEnv.init()
  }

  def getScanForSegments(
      @transient relation: HadoopFsRelation,
      output: Seq[Attribute],
      outputSchema: StructType,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression],
      tableIdentifier: Option[TableIdentifier]
  ): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output,
      outputSchema,
      partitionFilters,
      None,
      dataFilters,
      tableIdentifier)
  }

  def addSparkSessionListener(sparkSession: SparkSession): Unit = {
    sparkSession
      .sessionState
      .sessionStateListenerManager
      .addListener(new CloseSessionListener(sparkSession, sparkSession.sessionState))
  }
}

class CloseSessionListener(
    sparkSession: SparkSession,
    sessionState: SessionState
) extends SparkSessionListener {
  override def closeSession(): Unit = {
    CarbonEnv.carbonEnvMap.remove(sparkSession)
    ThreadLocalSessionInfo.unsetAll()
    sessionState.sessionStateListenerManager.removeListener(this)
  }
}
