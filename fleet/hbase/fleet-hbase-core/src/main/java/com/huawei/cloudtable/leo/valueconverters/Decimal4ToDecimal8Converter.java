package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

public final class Decimal4ToDecimal8Converter extends ValueConverter<Float, Double> implements ValueConverter.Implicit {

  public Decimal4ToDecimal8Converter() {
    super(Float.class, Double.class);
  }

  @Override
  public Double convert(final Float decimal4) throws ValueCodecException {
    return decimal4.doubleValue();
  }

}
