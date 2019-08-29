package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.value.Time;

public final class TimeType extends ValueType<Time>
    implements ValueType.Comparable<Time> {

  public static final Identifier NAME = Identifier.of("TIME");

  public static final Time SIMPLE = Time.of("00:00:00");

  public TimeType() {
    super(NAME, Time.class, null);
  }

  @Override
  public Time getMinimum() {
    return null;
  }

  @Override
  public Time getMaximum() {
    return null;
  }

  @Override
  public Time getSimple() {
    return SIMPLE;
  }

}
