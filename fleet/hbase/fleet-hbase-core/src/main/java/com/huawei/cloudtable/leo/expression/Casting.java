package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueConverter;

import javax.annotation.Nonnull;

public final class Casting<TSource, TTarget> extends Evaluation<TTarget> implements Function<TTarget> {

  public static final Identifier NAME = Identifier.of("CAST");

  public Casting(final Evaluation<TSource> source, final ValueConverter<TSource, TTarget> converter) {
    super(converter.getTargetClass(), source.isNullable(), source);
    this.converter = converter;
  }

  private final ValueConverter<TSource, TTarget> converter;

  public Evaluation<TSource> getSource() {
    return super.getParameter(0);
  }

  public ValueConverter<TSource, TTarget> getConverter() {
    return this.converter;
  }

  @Nonnull
  @Override
  public Identifier getName() {
    return NAME;
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  public TTarget evaluate(final RuntimeContext context) throws EvaluateException {
    return this.converter.convert(this.getSource().evaluate(context));
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    Function.toString(this, stringBuilder);
  }

}
