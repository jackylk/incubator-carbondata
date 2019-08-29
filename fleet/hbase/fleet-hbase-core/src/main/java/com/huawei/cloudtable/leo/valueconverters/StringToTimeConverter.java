package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;
import com.huawei.cloudtable.leo.value.Time;

public final class StringToTimeConverter extends ValueConverter<String, Time> implements ValueConverter.Implicit {

  public StringToTimeConverter() {
    super(String.class, Time.class);
  }

  @Override
  public Time convert(final String string) throws ValueCodecException {
    // TODO 如果格式不正确，会转换失败。
    return Time.of(string);
  }

}
