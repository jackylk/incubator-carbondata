package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

public final class StringType extends ValueType<String>
    implements ValueType.Comparable<String>, ValueType.Characters {

  public static final Identifier NAME = Identifier.of("STRING");

  public StringType() {
    super(NAME, String.class, null);
  }

  @Override
  public String getMinimum() {
    return null;
  }

  @Override
  public String getMaximum() {
    return null;
  }

  @Override
  public String getSimple() {
    return "S";
  }

}
