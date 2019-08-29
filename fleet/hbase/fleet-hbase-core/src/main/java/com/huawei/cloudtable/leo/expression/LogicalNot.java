package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;

public final class LogicalNot extends Evaluation<Boolean> implements Logical {

  public static final String NAME = "NOT";

  public LogicalNot(final Evaluation<Boolean> parameter) {
    super(Boolean.class, false, parameter);
    if (parameter == null) {
      throw new IllegalArgumentException();
    }
  }

  public Evaluation<Boolean> getParameter() {
    return super.getParameter(0);
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    return !this.getParameter().evaluate(context);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("NOT ");
    this.getParameter().toString(stringBuilder);
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  public <TResult, TParameter> TResult accept(final LogicalVisitor<TResult, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
