package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;

public final class Reference<TValue> extends DirectGet<TValue> {

  public Reference(
      final int attributeIndex,
      final Class<TValue> attributeClass,
      final boolean attributeNullable
  ) {
    super(attributeClass, attributeNullable);
    this.attributeIndex = attributeIndex;
  }

  private final int attributeIndex;

  public int getAttributeIndex() {
    return this.attributeIndex;
  }

  @Override
  public TValue evaluate(final RuntimeContext context) {
    return context.getAttribute(this);
  }

  @Override
  public ValueBytes evaluate(final RuntimeContext context, final ValueCodec<TValue> valueCodec) {
    return context.getAttributeAsBytes(this, valueCodec);
  }

  @Override
  public int hashCode() {
    return this.attributeIndex;
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
      final Reference that = (Reference) object;
      return this.attributeIndex == that.attributeIndex;
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("$").append(this.attributeIndex);
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
