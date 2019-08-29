package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigDecimal;

public final class Decimal8ToDecimalConverter extends ValueConverter<Double, BigDecimal> implements ValueConverter.Implicit {

  public Decimal8ToDecimalConverter() {
    super(Double.class, BigDecimal.class);
  }

  @Override
  public BigDecimal convert(final Double decimal8) throws ValueCodecException {
    return new BigDecimal(decimal8);
  }

}
