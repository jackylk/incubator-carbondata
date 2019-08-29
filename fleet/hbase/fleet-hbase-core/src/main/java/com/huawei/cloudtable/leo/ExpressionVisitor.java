package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.*;

public abstract class ExpressionVisitor<TResult, TParameter> {
  public abstract TResult visit(Case caze, TParameter parameter);

  public abstract TResult visit(Casting<?, ?> casting, TParameter parameter);

  public abstract TResult visit(Constant<?> constant, TParameter parameter);

  public abstract TResult visit(Reference<?> reference, TParameter parameter);

  public abstract TResult visit(Variable<?> variable, TParameter parameter);

  public abstract TResult visit(LikeExpression like, TParameter parameter);

  public abstract TResult visit(LogicalAnd logicalAnd, TParameter parameter);

  public abstract TResult visit(LogicalXor logicalXor, TParameter parameter);

  public abstract TResult visit(LogicalNot logicalNot, TParameter parameter);

  public abstract TResult visit(LogicalOr logicalOr, TParameter parameter);

  public abstract TResult visit(Function<?> function, TParameter parameter);

}
