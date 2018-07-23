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
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rule for rewriting plan if query has a filter on index table column
 */
class CarbonSITransformationRule(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  val secondaryIndexOptimizer: CarbonSecondaryIndexOptimizer =
    new CarbonSecondaryIndexOptimizer(sparkSession)

  def apply(plan: LogicalPlan): LogicalPlan = {
    val carbonLateDecodeRule = new CarbonLateDecodeRule
    if (carbonLateDecodeRule.checkIfRuleNeedToBeApplied(plan)) {
      secondaryIndexOptimizer.transformFilterToJoin(plan)
    } else {
      plan
    }
  }
}
