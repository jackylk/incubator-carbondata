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

package org.apache.spark.sql.execution.command.management

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.spark.rdd.CarbonOptimizeTableUtil

case class CarbonOptimizeTableCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: scala.collection.immutable.Map[String, String]) extends AtomicRunnableCommand {

  override protected def opName: String = "OPTIMIZE TABLE"

  var table: CarbonTable = _
  var segment: Segment = _
  var column: CarbonColumn = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    table = getTable(sparkSession)
    if (table.isPartitionTable || table.isHivePartitionTable) {
      throw new MalformedCarbonCommandException("not support partition table")
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    parseOptions()
    CarbonOptimizeTableUtil.repartitionSegment(sparkSession, table, segment, column)
    Seq.empty
  }

  private def getTable(sparkSession: SparkSession): CarbonTable = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      throw new NoSuchTableException(dbName, tableName)
    }
    if (null == relation.carbonTable) {
      CarbonOptimizeTableCommand.LOGGER
        .error(s"Data loading failed. table not found: $dbName.$tableName")
      throw new NoSuchTableException(dbName, tableName)
    }
    relation.carbonTable
  }

  def parseOptions() = {
    val segmentOption = options.get("segment")
    if (segmentOption.isEmpty) {
      throw new MalformedCarbonCommandException("not found segment option")
    }
    val segmentStatusManager = new SegmentStatusManager(table.getAbsoluteTableIdentifier)
    val segments = segmentStatusManager.getValidAndInvalidSegments()
    val validSegments = segments.getValidSegments
    val segmentNo = segmentOption.get
    val validSegment = validSegments.asScala.find(_.getSegmentNo.equals(segmentNo))
    if (validSegment.isEmpty) {
      throw new MalformedCarbonCommandException("need a valid segment option")
    }
    segment = validSegment.get

    val columnOption = options.get("partition_column")
    if (columnOption.isEmpty) {
      throw new MalformedCarbonCommandException("not found partition_column option")
    }
    column = table.getColumnByName(table.getTableName, columnOption.get)
    if (column == null) {
      throw new MalformedCarbonCommandException("need a valid partition_column option")
    }
  }
}

object CarbonOptimizeTableCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
}
