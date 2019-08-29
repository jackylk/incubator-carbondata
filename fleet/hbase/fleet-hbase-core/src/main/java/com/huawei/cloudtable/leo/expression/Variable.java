package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;

public final class Variable<TValue> extends DirectGet<TValue> {// TODO Object?

  public Variable(final Class<TValue> valueClass, final int index) {
    super(valueClass, true);
    if (index < 0) {
      throw new IllegalArgumentException("Variable [index] is less than 0.");
    }
    this.index = index;
  }

  private final int index;

  public int getIndex() {
    return this.index;
  }

  @Override
  public TValue evaluate(final RuntimeContext context) {
    return context.getVariable(this);
  }

  @Override
  public ValueBytes evaluate(final RuntimeContext context, final ValueCodec<TValue> valueCodec) {
    return context.getVariableAsBytes(this, valueCodec);
  }

  @Override
  public int hashCode() {
    return this.index;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (object.getClass() == this.getClass()) {
      final Variable that = (Variable) object;
      return this.index == that.index;
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append(":").append(Integer.valueOf(this.index));
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
