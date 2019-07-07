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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil


/**
 * User can add external data folder as a segment to a transactional table.
 * In case of external carbon data folder user no need to specify the format in options. But for
 * other formats like parquet user must specify the format=parquet in options.
 */
case class CarbonAddLoadCommand(
    databaseNameOp: Option[String],
    tableName: String,
    segmentPath: String,
    options: Option[Map[String, String]])
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)

    CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(model, false)

    val segment = new Segment(model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      segmentPath,
      options.map(o => new util.HashMap[String, String](o.asJava)).getOrElse(new util.HashMap()))
    val isSuccess =
      SegmentFileStore.writeSegmentFile(carbonTable, segment)

    if (isSuccess) {
      SegmentFileStore.updateSegmentFile(
        carbonTable,
        model.getSegmentId,
        segment.getSegmentFileName,
        carbonTable.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName),
        SegmentStatus.SUCCESS)
    } else {
      throw new AnalysisException("Adding segment with path failed.")
    }
    Seq.empty
  }

  override protected def opName: String = "ADD SEGMENT WITH PATH"
}
