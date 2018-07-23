/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
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

  public static final String USER_NAME = "user_name";

  public static final String USER_UNIQUE_UGI_OBJECT = "userUniqueUGIObject";

}