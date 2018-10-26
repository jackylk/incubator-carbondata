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
package org.apache.carbondata.core.util;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.annotations.CarbonProperty;
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants;

import org.apache.log4j.Logger;

/**
 *
 */
public final class CarbonPluginProperties {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final Logger LOGGER =
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
