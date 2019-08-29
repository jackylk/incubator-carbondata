package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Decimal4Type extends ValueType<Float>
    implements ValueType.Comparable<Float>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("DECIMAL4");

  public Decimal4Type() {
    super(NAME, Float.class, null);
  }

  @Override
  public Float getMinimum() {
    return Float.MIN_VALUE;
  }

  @Override
  public Float getMaximum() {
    return Float.MAX_VALUE;
  }

  @Override
  public Float getSimple() {
    return 0.0F;
  }

}
