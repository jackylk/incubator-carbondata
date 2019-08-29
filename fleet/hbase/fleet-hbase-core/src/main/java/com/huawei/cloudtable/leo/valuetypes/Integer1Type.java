package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class Integer1Type extends ValueType<Byte>
    implements ValueType.Comparable<Byte>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("INTEGER1");

  public Integer1Type() {
    super(NAME, Byte.class, null);
  }

  @Override
  public Byte getMinimum() {
    return Byte.MIN_VALUE;
  }

  @Override
  public Byte getMaximum() {
    return Byte.MAX_VALUE;
  }

  @Override
  public Byte getSimple() {
    return 0;
  }

}
