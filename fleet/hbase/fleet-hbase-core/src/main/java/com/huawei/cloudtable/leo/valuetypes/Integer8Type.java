package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Integer8Type extends ValueType<Long>
    implements ValueType.Comparable<Long>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("INTEGER8");

  public Integer8Type() {
    super(NAME, Long.class, null);
  }

  @Override
  public Long getMinimum() {
    return Long.MIN_VALUE;
  }

  @Override
  public Long getMaximum() {
    return Long.MAX_VALUE;
  }

  @Override
  public Long getSimple() {
    return 0L;
  }

}
