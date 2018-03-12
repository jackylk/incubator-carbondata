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

package org.apache.carbondata.spark.core;

import org.apache.carbondata.core.util.CarbonProperty;

public final class CarbonCommonPluginConstants {

  /**
   * It is internal configuration and used only for test purpose.
   * It will merge the carbon index files with in the segment to single segment.
   */
  @CarbonProperty
  public static final String CARBON_MERGE_INDEX_IN_SEGMENT = "carbon.merge.index.in.segment";

  public static final String CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT = "true";

}