package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;
import com.huawei.cloudtable.leo.value.Timestamp;

public final class StringToTimestampConverter extends ValueConverter<String, Timestamp> implements ValueConverter.Implicit {

  public StringToTimestampConverter() {
    super(String.class, Timestamp.class);
  }

  @Override
  public Timestamp convert(final String string) throws ValueCodecException {
    // TODO 如果格式不正确，会转换失败。
    return Timestamp.valueOf(string);
  }

}
