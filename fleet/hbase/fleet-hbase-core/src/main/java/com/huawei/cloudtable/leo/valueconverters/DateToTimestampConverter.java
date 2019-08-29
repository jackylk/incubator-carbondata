package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;
import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Timestamp;

public final class DateToTimestampConverter extends ValueConverter<Date, Timestamp> implements ValueConverter.Implicit {

  public DateToTimestampConverter() {
    super(Date.class, Timestamp.class);
  }

  @Override
  public Timestamp convert(final Date date) throws ValueCodecException {
    return Timestamp.valueOf(date.getTime());
  }

}
