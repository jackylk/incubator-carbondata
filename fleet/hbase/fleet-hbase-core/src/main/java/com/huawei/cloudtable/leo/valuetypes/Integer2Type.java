package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Integer2Type extends ValueType<Short>
    implements ValueType.Comparable<Short>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("INTEGER2");

  public Integer2Type() {
    super(NAME, Short.class, null);
  }

  @Override
  public Short getMinimum() {
    return Short.MIN_VALUE;
  }

  @Override
  public Short getMaximum() {
    return Short.MAX_VALUE;
  }

  @Override
  public Short getSimple() {
    return 0;
  }

}
