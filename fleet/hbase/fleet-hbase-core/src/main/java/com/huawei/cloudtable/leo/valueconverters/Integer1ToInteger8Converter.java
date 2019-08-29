package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer1ToInteger8Converter extends ValueConverter<Byte, Long> implements ValueConverter.Implicit {

  public Integer1ToInteger8Converter() {
    super(Byte.class, Long.class);
  }

  @Override
  public Long convert(final Byte integer1) throws ValueCodecException {
    return integer1.longValue();
  }

}
