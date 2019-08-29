package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer2ToInteger8Converter extends ValueConverter<Short, Long> implements ValueConverter.Implicit {

  public Integer2ToInteger8Converter() {
    super(Short.class, Long.class);
  }

  @Override
  public Long convert(final Short integer2) throws ValueCodecException {
    return integer2.longValue();
  }

}
