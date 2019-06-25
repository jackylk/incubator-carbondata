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
package org.apache.spark.util

import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.execution.command.CreateFunctionCommand

/**
 * reflection utils class for private code
 */
object CarbonInternalReflectionUtils {

  def callCreateFunctionCommand(databaseName: Option[String],
    functionName: String,
    className: String,
    resources: Seq[FunctionResource],
    isTemp: Boolean): CreateFunctionCommand = {
    val caseClassName = "org.apache.spark.sql.execution.command.CreateFunctionCommand"
    if (SparkUtil.isSparkVersionEqualTo("2.1") || SparkUtil.isSparkVersionEqualTo("2.2")) {
      val createFunction: (Any, Class[_]) = CarbonReflectionUtils.createObject(caseClassName,
        databaseName,
        functionName,
        className,
        resources,
        Predef.boolean2Boolean(isTemp))
      createFunction._1.asInstanceOf[CreateFunctionCommand]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val createFunction: (Any, Class[_]) = CarbonReflectionUtils.createObject(caseClassName,
        databaseName,
        functionName,
        className,
        resources,
        Predef.boolean2Boolean(isTemp),
        Predef.boolean2Boolean(false),
        Predef.boolean2Boolean(true))
      // Assuming that the create function is in shared state and takes functions from previous
      // state's functionRegistry in Spark2.3, we make the last flag of the above function as
      // true which is for replaceIfExists that function.
      createFunction._1.asInstanceOf[CreateFunctionCommand]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }
}
