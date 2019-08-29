package com.huawei.cloudtable.leo.expression;

public interface Logical {

  <TResult, TParameter> TResult accept(LogicalVisitor<TResult, TParameter> visitor, TParameter parameter);

}
