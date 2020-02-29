package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object AnalyzerUtil {

  def transform(plan: LogicalPlan)
    (rule: scala.PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    plan.transform(rule)
  }

}
