package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ValueRange;

import javax.annotation.Nullable;

public interface RangeExpression<TValue extends Comparable<TValue>> {

  int getParameterCount();

  @Nullable
  <TParameter> Evaluation<TParameter> getParameter(int parameterIndex);

  ValueRange<TValue> getValueRange(int variableIndex);

  ValueRange<TValue> getValueRange(int variableIndex, Evaluation.RuntimeContext context);

}
