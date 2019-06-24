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

package org.apache.carbondata.vector.column;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.vector.file.FileConstants;
import org.apache.carbondata.vector.file.reader.ArrayReader;
import org.apache.carbondata.vector.file.reader.ArrayReaderFactory;
import org.apache.carbondata.vector.file.vector.ArrayVector;
import org.apache.carbondata.vector.file.vector.ArrayVectorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * column reader for vector table
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public class VectorColumnReader extends RecordReader<Void, Object> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(VectorColumnReader.class.getCanonicalName());

  private final boolean enableBatch;
  private final CarbonTable table;
  private final CarbonColumn column;
  private final Configuration hadoopConf;
  private String segmentPath;
  private ColumnarBatch columnarBatch;
  private int batchSize = -1;
  private int batchIndex = -1;
  private ArrayVector[] columnData;
  private ArrayReader reader;

  public VectorColumnReader(
      final CarbonTable table,
      final CarbonColumn column,
      final Configuration hadoopConf,
      final boolean enableBatch) {
    this.table = table;
    this.column = column;
    this.hadoopConf = hadoopConf;
    this.enableBatch = enableBatch;
  }

  /**
   *
   * @param inputSplit
   * @param taskAttemptContext
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    CarbonInputSplit carbonInputSplit = ((CarbonMultiBlockSplit) inputSplit).getAllSplits().get(0);
    columnData = new ArrayVector[] { ArrayVectorFactory.createArrayVector(column) };
    columnarBatch = new ColumnarBatch(columnData);

    segmentPath = CarbonTablePath
        .getSegmentPath(table.getTablePath(), carbonInputSplit.getSegment().getSegmentNo());
    reader = ArrayReaderFactory.createArrayReader(table, column);
    reader.open(segmentPath, hadoopConf);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (enableBatch) {
      return nextBatch();
    }
    batchIndex++;
    if (batchIndex >= batchSize) {
      if (!nextBatch()) return false;
    }
    return true;
  }

  /**
   * fill next batch
   * @return
   * @throws IOException
   */
  private boolean nextBatch() throws IOException {
    int rowCount;
    rowCount = fillVector();
    columnarBatch.setNumRows(rowCount);
    batchSize = rowCount;
    batchIndex = 0;
    return rowCount != -1;
  }

  private int fillVector() throws IOException {
    int rowCount = 0;
    do {
      rowCount = columnData[0].fillVector(reader, FileConstants.FILE_READ_BACTH_ROWS);
      if (rowCount == -1) {
        // release reader resource
        closeReader();
        break;
      }
    } while (rowCount == 0);
    return rowCount;
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (enableBatch) {
      return columnarBatch;
    } else {
      return columnarBatch.getRow(batchIndex);
    }
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  /**
   * close all file readers to release resource
   * @throws IOException
   */
  private void closeReader() throws IOException {
    IOException ex = ArrayReaderFactory.destroyArrayReader(
        "Failed to close array file reader",
        reader);
    reader = null;
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * clean the data in this record reader
   */
  private void cleanData() {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (columnData != null) {
      columnData[0] = null;
      columnData = null;
    }
  }

  @Override
  public void close() throws IOException {
    cleanData();
    closeReader();
  }
}
