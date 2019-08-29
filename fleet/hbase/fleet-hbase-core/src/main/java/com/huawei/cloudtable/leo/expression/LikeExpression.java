package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;

public final class LikeExpression extends Evaluation<Boolean> {

  public LikeExpression(final Evaluation<String> valueParameter, final Evaluation<String> patternParameter) {
    super(Boolean.class, false, valueParameter, patternParameter);
    if (valueParameter == null) {
      throw new IllegalArgumentException("Argument [valueParameter] is null.");
    }
    if (patternParameter == null) {
      throw new IllegalArgumentException("Argument [patternParameter] is null.");
    }
  }

  public Evaluation<String> getValueParameter() {
    return this.getParameter(0);
  }

  public Evaluation<String> getPatternParameter() {
    return this.getParameter(1);
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.getValueParameter().toString(stringBuilder);
    stringBuilder.append(" LIKE ");
    this.getPatternParameter().toString(stringBuilder);
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
