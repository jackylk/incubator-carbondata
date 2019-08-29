package com.huawei.cloudtable.leo.expression;

public interface LogicalVisitor<TResult, TParameter> {

  TResult visit(LogicalNot not, TParameter parameter);

  TResult visit(LogicalAnd and, TParameter parameter);

  TResult visit(LogicalOr or, TParameter parameter);

  TResult visit(LogicalXor xor, TParameter parameter);

}
