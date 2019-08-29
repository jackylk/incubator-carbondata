package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class BooleanType extends ValueType<Boolean>
    implements ValueType.Comparable<Boolean> {

  public static final Identifier NAME = Identifier.of("BOOLEAN");

  public BooleanType() {
    super(NAME, Boolean.class, null);
  }

  @Override
  public Boolean getMinimum() {
    return Boolean.FALSE;
  }

  @Override
  public Boolean getMaximum() {
    return Boolean.TRUE;
  }

  @Override
  public Boolean getSimple() {
    return Boolean.FALSE;
  }

}
