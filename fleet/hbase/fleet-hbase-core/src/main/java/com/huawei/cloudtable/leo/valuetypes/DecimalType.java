package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

import java.math.BigDecimal;

public final class DecimalType extends ValueType<BigDecimal>
    implements ValueType.Comparable<BigDecimal>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("DECIMAL");

  public DecimalType() {
    super(NAME, BigDecimal.class, null);
  }

  @Override
  public BigDecimal getMinimum() {
    return null;
  }

  @Override
  public BigDecimal getMaximum() {
    return null;
  }

  @Override
  public BigDecimal getSimple() {
    return BigDecimal.ZERO;
  }

}
