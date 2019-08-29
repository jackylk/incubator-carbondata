package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigDecimal;

public final class Decimal4ToDecimalConverter extends ValueConverter<Float, BigDecimal> implements ValueConverter.Implicit {

  public Decimal4ToDecimalConverter() {
    super(Float.class, BigDecimal.class);
  }

  @Override
  public BigDecimal convert(final Float decimal4) throws ValueCodecException {
    return new BigDecimal(decimal4);
  }

}
