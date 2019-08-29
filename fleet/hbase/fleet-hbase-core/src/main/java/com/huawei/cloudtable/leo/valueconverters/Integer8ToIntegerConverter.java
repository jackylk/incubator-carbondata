package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigInteger;

public final class Integer8ToIntegerConverter extends ValueConverter<Long, BigInteger> implements ValueConverter.Implicit {

  public Integer8ToIntegerConverter() {
    super(Long.class, BigInteger.class);
  }

  @Override
  public BigInteger convert(final Long integer8) throws ValueCodecException {
    return BigInteger.valueOf(integer8);
  }

}
