package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.value.Date;

public final class DateType extends ValueType<Date>
    implements ValueType.Comparable<Date> {

  public static final Identifier NAME = Identifier.of("DATE");

  public static final Date SIMPLE = Date.of("1970-01-01");

  public DateType() {
    super(NAME, Date.class, null);
  }

  @Override
  public Date getMinimum() {
    return null;
  }

  @Override
  public Date getMaximum() {
    return null;
  }

  @Override
  public Date getSimple() {
    return SIMPLE;
  }

}
