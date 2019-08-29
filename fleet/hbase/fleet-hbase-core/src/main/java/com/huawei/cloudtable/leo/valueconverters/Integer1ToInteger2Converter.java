package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer1ToInteger2Converter extends ValueConverter<Byte, Short> implements ValueConverter.Implicit {

  public Integer1ToInteger2Converter() {
    super(Byte.class, Short.class);
  }

  @Override
  public Short convert(final Byte integer1) throws ValueCodecException {
    return integer1.shortValue();
  }

}
