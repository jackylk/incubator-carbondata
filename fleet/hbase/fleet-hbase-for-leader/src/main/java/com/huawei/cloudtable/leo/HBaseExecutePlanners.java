package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.planners.InsertPlanner;
import com.huawei.cloudtable.leo.planners.SelectPlanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HBaseExecutePlanners {

  @SuppressWarnings("unchecked")
  static <TExecuteResult, TStatement extends Statement<TExecuteResult>> HBaseExecutePlanner<TExecuteResult, TStatement> getPlanner(
      final TStatement statement
  ) {
    return (HBaseExecutePlanner<TExecuteResult, TStatement>)PLANNER_MAP.get(statement.getClass());
  }

  private static final Map<Class<?>, HBaseExecutePlanner<?, ?>> PLANNER_MAP;

  static {
    final List<HBaseExecutePlanner<?, ?>> plannerList = new ArrayList<>();
    plannerList.add(new SelectPlanner());
    plannerList.add(new InsertPlanner.FromValues());
    final Map<Class<?>, HBaseExecutePlanner<?, ?>> plannerMap = new HashMap<>(plannerList.size());
    for (HBaseExecutePlanner<?, ?> planner : plannerList) {
      plannerMap.put(planner.getStatementClass(), planner);
    }
    PLANNER_MAP = plannerMap;
  }

  private HBaseExecutePlanners() {
    // to do nothing.
  }

}
