package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;

public final class Constant<TValue> extends DirectGet<TValue> {

  public Constant(final Class<TValue> valueClass, final TValue value) {
    this(valueClass, value, null);
  }

  public Constant(final Class<TValue> valueClass, final TValue value, final String valueString) {
    super(valueClass, false);
    if (value == null) {
      throw new IllegalArgumentException("Variable [value] is null.");
    }
    this.value = value;
    this.valueString = valueString;
  }

  private final TValue value;

  private final String valueString;

  private ValueCodec<TValue> valueCodec;

  private ValueBytes valueBytes;

  public TValue getValue() {
    return this.value;
  }

  public String getValueString() {
    return this.valueString;
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
    final ValueCodec<TValue> valueCodec = context.getValueCodec(this.getResultClass());
    final ValueBytes valueBytes = valueCodec == null ? null : ValueBytes.of(valueCodec.encode(value));
    this.valueCodec = valueCodec;
    this.valueBytes = valueBytes;
  }

  @Override
  public TValue evaluate(final RuntimeContext context) {
    return this.value;
  }

  @Override
  public ValueBytes evaluate(final RuntimeContext context, final ValueCodec<TValue> valueCodec) {
    if (this.valueBytes != null && this.valueCodec == valueCodec) {
      return this.valueBytes;
    } else {
      return ValueBytes.of(valueCodec.encode(this.value));
    }
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
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
      final Constant that = (Constant) object;
      return this.value.equals(that.value);
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    if (this.valueString == null) {
      stringBuilder.append(this.value.toString());
    } else {
      stringBuilder.append(this.valueString);
    }
  }

}
