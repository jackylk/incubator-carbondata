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
  private final CarbonColumn column;
  private final String segmentPath;
  private ArrayWriter arrayWriter;
  private boolean isFirstRow = true;

  public VectorColumnWriter(
      CarbonTable table, CarbonColumn column, String segmentNo, Configuration hadoopConf) {
    this.table = table;
    this.hadoopConf = hadoopConf;
    this.segmentPath = CarbonTablePath.getSegmentPath(table.getTablePath(), segmentNo);
    this.column = column;
  }

  private synchronized void initWriter() throws VectorTableException {
    if (isFirstRow) {
      isFirstRow = false;
      // init writer
      try {
        arrayWriter = ArrayWriterFactory.createArrayWriter(table, column);
        arrayWriter.open(segmentPath, hadoopConf);
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
  public void write(Object value) throws VectorTableException {
    if (isFirstRow) {
      initWriter();
    }
    try {
      arrayWriter.appendObject(value);
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
        arrayWriter);
    arrayWriter = null;
    if (ex != null) {
      throw new VectorTableException("Failed to close writer for insert column");
    }
  }
}
