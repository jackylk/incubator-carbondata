package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Integer2ToInteger4Converter extends ValueConverter<Short, Integer> implements ValueConverter.Implicit {

  public Integer2ToInteger4Converter() {
    super(Short.class, Integer.class);
  }

  @Override
  public Integer convert(final Short integer2) throws ValueCodecException {
    return integer2.intValue();
  }

}
