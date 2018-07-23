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
package org.apache.carbondata.spark.spark.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NonIndexUDFExpression extends UDF {
  public Boolean evaluate(Object input) {
    return true;
  }
}

