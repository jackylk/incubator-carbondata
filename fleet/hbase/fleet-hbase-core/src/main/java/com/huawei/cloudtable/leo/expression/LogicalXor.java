package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;

public final class LogicalXor extends Evaluation<Boolean>
    implements Logical, Commutative<Boolean, Boolean> {

  public LogicalXor(final Evaluation<Boolean> parameter1, final Evaluation<Boolean> parameter2) {
    super(Boolean.class, false, parameter1, parameter2);
    if (parameter1 == null || parameter2 == null) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Class<Boolean> getParameter1Class() {
    return Boolean.class;
  }

  @Override
  public Class<Boolean> getParameter2Class() {
    return Boolean.class;
  }

  @Override
  public Evaluation<Boolean> getParameter1() {
    return super.getParameter(0);
  }

  @Override
  public Evaluation<Boolean> getParameter2() {
    return super.getParameter(1);
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    return this.getParameter1().evaluate(context) | this.getParameter2().evaluate(context);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.getParameter1().toString(stringBuilder);
    stringBuilder.append(" XOR ");
    this.getParameter2().toString(stringBuilder);
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
