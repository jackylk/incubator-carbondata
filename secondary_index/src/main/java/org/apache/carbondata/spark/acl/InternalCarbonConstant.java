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
package org.apache.carbondata.spark.acl;

import org.apache.carbondata.core.util.annotations.CarbonProperty;

/**
 * Internal huawei constants
 */
public class InternalCarbonConstant {

  /**
   * Constant for shared column
   */
  public static final String SHARED_COLUMN = "shared_column";

  /**
   * CARBON_DATALOAD_GROUP_NAME
   */
  @CarbonProperty
  public static final String CARBON_DATALOAD_GROUP_NAME = "carbon.dataload.group.name";

  /**
   * CARBON_DATALOAD_GROUP_NAME_DEFAULT
   */
  public static final String CARBON_DATALOAD_GROUP_NAME_DEFAULT = "carbondataload";
  /**
   * min percentage of resource needed for the scheduling the task
   */
  @CarbonProperty
  public static final String CARBON_SCHEDULER_MINREGISTEREDRESOURCESRATIO =
      "carbon.scheduler.minRegisteredResourcesRatio";
  /**
   * default percentage value of resource needed for the scheduling the task
   */
  public static final String CARBON_SCHEDULER_MINREGISTEREDRESOURCESRATIO_DEFAULT = "0.8d";

  /**
   * To define how much time scheduler should wait for the
   * resource in dynamic allocation.
   */
  @CarbonProperty
  public static final String CARBON_DYNAMICALLOCATION_SCHEDULERTIMEOUT =
      "carbon.dynamicAllocation.schedulerTimeout";

  /**
   * default scheduler wait time
   */
  public static final String CARBON_DYNAMICALLOCATION_SCHEDULERTIMEOUT_DEFAULT = "5";

  @CarbonProperty
  public static final String CARBON_ACCESS_CONTROL_RULE_ENABLED =
      "carbon.access.control.rule.enabled";

  /**
   * CARBON_ACCESS_CONTROL_RULE_ENABLED_DEFAULT
   */
  public static final String CARBON_ACCESS_CONTROL_RULE_ENABLED_DEFAULT = "false";

}
