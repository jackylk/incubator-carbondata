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

package org.apache.carbondata.api;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.statusmanager.FileFormat;

@InterfaceAudience.User
@InterfaceStability.Evolving
public interface SegmentManager {

  /**
   * Return new segment Id created
   *
   * @param format format of the segment data files
   * @return newly created segment id
   * @throws IOException if IO error occurred
   */
  String createSegment(FileFormat format) throws IOException;

  /**
   * If the passing online segment is full, trigger 'handoff'
   * operation, create and return a new online segment path.
   * Otherwise return the passing path
   *
   * @param segmentPath path to the segment data files
   * @param format format of the segment data files
   * @throws IOException if IO error occurred
   */
  String getOrCreateSegment(String segmentPath, FileFormat format) throws IOException;

  /**
   * write segment to be table meta path and mark segment status as Success
   *
   * @param segmentId to which segment has to be written to table meta path
   * @return commit status
   * @throws IOException if IO error occurred
   */
  boolean commitSegment(String segmentId) throws IOException;

  /**
   * delete corresponding segment from the table
   * @param segmentId to be deleted
   * @throws IOException if IO error occurred
   */
  void deleteSegment(String segmentId) throws IOException;
}
