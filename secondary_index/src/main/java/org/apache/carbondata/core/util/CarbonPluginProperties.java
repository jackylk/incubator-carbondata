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
package org.apache.carbondata.core.util;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants;

/**
 *
 */
public final class CarbonPluginProperties {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonProperties.class.getName());

  /**
   * validate and register Carbon internal properties
   *
   */
  public static void validateAndLoadDefaultInternalProperties() throws IllegalAccessException {
    initInternalPropertySet();

    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    loadDefaulValue(carbonProperties);
    validateHighCardinalityThreshold(carbonProperties);
  }

  private static void loadDefaulValue(CarbonProperties carbonProperties) {
    String isPartialStringEnabled = carbonProperties
        .getProperty(CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
            CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT);
    if (isPartialStringEnabled.equalsIgnoreCase("true") || isPartialStringEnabled
        .equalsIgnoreCase("false")) {
      carbonProperties.addProperty(CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
          isPartialStringEnabled);
    } else {
      carbonProperties.addProperty(CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
          CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT);
    }
  }

  private static void validateHighCardinalityThreshold(CarbonProperties carbonProperties) {
    String highcardThresholdStr = carbonProperties
        .getProperty(CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD,
            CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
    try {
      int highcardThreshold = Integer.parseInt(highcardThresholdStr);
      if (highcardThreshold < CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN) {
        LOGGER.info("The high cardinality threshold value \"" + highcardThresholdStr
            + "\" is invalid. Using the min value \""
            + CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN);
        carbonProperties.addProperty(CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD,
            CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_MIN + "");
      }
    } catch (NumberFormatException e) {
      LOGGER.info("The high cardinality threshold value \"" + highcardThresholdStr
          + "\" is invalid. Using the default value \""
          + CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
      carbonProperties.addProperty(CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD,
          CarbonInternalCommonConstants.HIGH_CARDINALITY_THRESHOLD_DEFAULT);
    }
  }

  private static void initInternalPropertySet()throws IllegalAccessException {
    Field[] declaredFields = CarbonInternalCommonConstants.class.getDeclaredFields();
    Set<String> propertySet =  new HashSet<String>();
    for (Field field : declaredFields) {
      if (field.isAnnotationPresent(CarbonProperty.class)) {
        propertySet.add(field.get(field.getName()).toString());
      }
    }
    CarbonProperties.getInstance().addPropertyToPropertySet(propertySet);
  }
}
