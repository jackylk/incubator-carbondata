package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.Function;
import javax.annotation.Nonnull;

public abstract class Expression<TResult> {

  public Expression(final Class<TResult> resultClass, final boolean nullable) {
    if (resultClass == null) {
      throw new IllegalArgumentException("Argument [resultClass] is null.");
    }
    this.resultClass = resultClass;
    this.nullable = nullable;
  }

  private final Class<TResult> resultClass;

  private final boolean nullable;

  @Nonnull
  public final Class<TResult> getResultClass() {
    return this.resultClass;
  }

  public final boolean isNullable() {
    return this.nullable;
  }

  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    if (!(this instanceof Function)) {
      throw new UnsupportedOperationException(this.getClass().getName());
    }
    return visitor.visit((Function<?>) this, parameter);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object object);

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    this.toString(stringBuilder);
    return stringBuilder.toString();
  }

  public abstract void toString(StringBuilder stringBuilder);

}
