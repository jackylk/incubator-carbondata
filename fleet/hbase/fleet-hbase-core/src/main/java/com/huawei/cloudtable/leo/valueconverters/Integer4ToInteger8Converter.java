package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer4ToInteger8Converter extends ValueConverter<Integer, Long> implements ValueConverter.Implicit {

  public Integer4ToInteger8Converter() {
    super(Integer.class, Long.class);
  }

  @Override
  public Long convert(final Integer integer4) throws ValueCodecException {
    return integer4.longValue();
  }

}
