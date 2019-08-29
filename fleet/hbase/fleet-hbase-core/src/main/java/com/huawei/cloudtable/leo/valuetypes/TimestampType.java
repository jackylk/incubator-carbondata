package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.value.Timestamp;

public final class TimestampType extends ValueType<Timestamp>
    implements ValueType.Comparable<Timestamp> {

  public static final Identifier NAME = Identifier.of("TIMESTAMP");

  public static final Timestamp SIMPLE = Timestamp.valueOf("1970-01-01 00:00:00.000");

  public TimestampType() {
    super(NAME, Timestamp.class, null);
  }

  @Override
  public Timestamp getMinimum() {
    return null;
  }

  @Override
  public Timestamp getMaximum() {
    return null;
  }

  @Override
  public Timestamp getSimple() {
    return SIMPLE;
  }

}
