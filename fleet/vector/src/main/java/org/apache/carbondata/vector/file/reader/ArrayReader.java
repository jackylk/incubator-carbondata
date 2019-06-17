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

package org.apache.carbondata.vector.file.reader;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

import org.apache.hadoop.conf.Configuration;

/**
 * interface to read array data file
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public interface ArrayReader extends AutoCloseable {

  /**
   * open the reader and init the configuration
   * @param inputFolder
   * @param hadoopConf
   */
  void open(String inputFolder, Configuration hadoopConf) throws IOException;

  /**
   * close the reader and release resources
   */
  void close() throws IOException;
}
