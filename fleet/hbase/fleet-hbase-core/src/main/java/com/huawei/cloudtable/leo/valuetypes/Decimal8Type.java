package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Decimal8Type extends ValueType<Double>
    implements ValueType.Comparable<Double>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("DECIMAL8");

  public Decimal8Type() {
    super(NAME, Double.class, null);
  }

  @Override
  public Double getMinimum() {
    return Double.MIN_VALUE;
  }

  @Override
  public Double getMaximum() {
    return Double.MAX_VALUE;
  }

  @Override
  public Double getSimple() {
    return 0.0;
  }

}
