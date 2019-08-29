package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;
import com.huawei.cloudtable.leo.value.Date;

public final class StringToDateConverter extends ValueConverter<String, Date> implements ValueConverter.Implicit {

  public StringToDateConverter() {
    super(String.class, Date.class);
  }

  @Override
  public Date convert(final String string) throws ValueCodecException {
    // TODO 如果格式不正确，会转换失败。
    return Date.of(string);
  }

}
