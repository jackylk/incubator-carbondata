package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class IntegerToDecimalConverter extends ValueConverter<BigInteger, BigDecimal> implements ValueConverter.Implicit {

  public IntegerToDecimalConverter() {
    super(BigInteger.class, BigDecimal.class);
  }

  @Override
  public BigDecimal convert(final BigInteger integer) throws ValueCodecException {
    return new BigDecimal(integer);
  }

}
