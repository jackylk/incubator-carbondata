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

package org.apache.carbondata.spark.acl;

import org.apache.carbondata.core.util.CarbonProperty;

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
