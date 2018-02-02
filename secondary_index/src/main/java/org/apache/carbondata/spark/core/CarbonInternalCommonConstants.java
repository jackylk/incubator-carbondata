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

public final class CarbonInternalCommonConstants {
  /**
   * to enable SI lookup partial string
   */
  @CarbonProperty
  public static final String ENABLE_SI_LOOKUP_PARTIALSTRING = "carbon.si.lookup.partialstring";

  /**
   * default value of ENABLE_SI_LOOKUP_PARTIALSTRING
   */
  public static final String ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT = "true";

  /**
   * configuration for launching the number of threads during secondary index creation
   */
  @CarbonProperty
  public static final String CARBON_SECONDARY_INDEX_CREATION_THREADS =
      "carbon.secondary.index.creation.threads";

  /**
   * default value configuration for launching the number of threads during secondary
   * index creation
   */
  public static final String CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT = "1";

  /**
   * max value configuration for launching the number of threads during secondary
   * index creation
   */
  public static final int CARBON_SECONDARY_INDEX_CREATION_THREADS_MAX = 50;

  public static final String POSITION_REFERENCE = "positionReference";

  public static final String POSITION_ID = "positionId";

  @CarbonProperty
  public static final String CARBON_UPDATE_SYNC_FOLDER = "carbon.update.sync.folder";
  public static final String CARBON_UPDATE_SYNC_FOLDER_DEFAULT = "/tmp/carbondata";

  /**
   * threshold of high cardinality
   */
  @CarbonProperty
  public static final String HIGH_CARDINALITY_THRESHOLD = "high.cardinality.threshold";
  public static final String HIGH_CARDINALITY_THRESHOLD_DEFAULT = "1000000";
  public static final int HIGH_CARDINALITY_THRESHOLD_MIN = 10000;

  @CarbonProperty
  public static final String CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER =
      "carbon.infilter.subquery.pushdown.enable";


  /**
   * CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT
   */
  public static final String CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT = "false";


  /**
   * key to get broadcast record size from properties
   */
  @CarbonProperty
  public static final String BROADCAST_RECORD_SIZE = "broadcast.record.size";

  /**
   * default broadcast record size
   */
  public static final String DEFAULT_BROADCAST_RECORD_SIZE = "100";

  /**
   * It is internal configuration and used only for test purpose.
   * It will merge the carbon index files with in the segment to single segment.
   */
  public static final String CARBON_MERGE_INDEX_IN_SEGMENT = "carbon.merge.index.in.segment";

  public static final String CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT = "true";

}