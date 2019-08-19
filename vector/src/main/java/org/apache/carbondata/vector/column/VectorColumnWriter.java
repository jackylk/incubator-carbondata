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
import org.apache.carbondata.vector.exception.VectorTableException;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.file.writer.ArrayWriterFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * writer column API for vector table
 */
@InterfaceAudience.User
@InterfaceStability.Stable
public class VectorColumnWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(VectorColumnWriter.class.getCanonicalName());

  private final Configuration hadoopConf;
  private final CarbonTable table;
  private final CarbonColumn[] columns;
  private final int numColumns;
  private final String segmentPath;
  private ArrayWriter[] arrayWriters;
  private boolean isFirstRow = true;

  public VectorColumnWriter(
      CarbonTable table, CarbonColumn[] columns, String segmentNo, Configuration hadoopConf) {
    this.table = table;
    this.hadoopConf = hadoopConf;
    this.segmentPath = CarbonTablePath.getSegmentPath(table.getTablePath(), segmentNo);
    this.columns = columns;
    this.numColumns = columns.length;
    this.arrayWriters = new ArrayWriter[numColumns];
  }

  private synchronized void initWriter() throws VectorTableException {
    if (isFirstRow) {
      isFirstRow = false;
      // init writer
      try {
        for (int index = 0; index < numColumns; index++) {
          arrayWriters[index] = ArrayWriterFactory.createArrayWriter(table, columns[index]);
          arrayWriters[index].open(segmentPath, hadoopConf);
        }
      } catch (IOException e) {
        String message = "Failed to init array writer";
        LOGGER.error(message, e);
        throw new VectorTableException(message);
      }
    }
  }

  /**
   * write one value of the column
   */
  public void write(Object[] value) throws VectorTableException {
    if (isFirstRow) {
      initWriter();
    }
    try {
      for (int index = 0; index < numColumns; index++) {
        arrayWriters[index].appendObject(value[index]);
      }
    } catch (IOException e) {
      String message = "Failed to write column";
      LOGGER.error(message, e);
      throw new VectorTableException(message);
    }
  }

  /**
   * finally release resource
   */
  public void close() throws VectorTableException {
    IOException ex = ArrayWriterFactory.destroyArrayWriter(
        "Failed to close array file writer for insert column",
        arrayWriters);
    if (arrayWriters != null) {
      for (int index = 0; index < numColumns; index++) {
        arrayWriters[index] = null;
      }
      arrayWriters = null;
    }
    if (ex != null) {
      throw new VectorTableException("Failed to close writer for insert column");
    }
  }
}
