package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Integer4Type extends ValueType<Integer>
    implements ValueType.Comparable<Integer>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("INTEGER4");

  public Integer4Type() {
    super(NAME, Integer.class, null);
  }

  @Override
  public Integer getMinimum() {
    return Integer.MIN_VALUE;
  }

  @Override
  public Integer getMaximum() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Integer getSimple() {
    return 0;
  }

}
