package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer1ToInteger4Converter extends ValueConverter<Byte, Integer> implements ValueConverter.Implicit {

  public Integer1ToInteger4Converter() {
    super(Byte.class, Integer.class);
  }

  @Override
  public Integer convert(final Byte integer1) throws ValueCodecException {
    return integer1.intValue();
  }

}
