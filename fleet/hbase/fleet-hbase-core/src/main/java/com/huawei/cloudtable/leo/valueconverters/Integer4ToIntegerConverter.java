package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigInteger;

public final class Integer4ToIntegerConverter extends ValueConverter<Integer, BigInteger> implements ValueConverter.Implicit {

  public Integer4ToIntegerConverter() {
    super(Integer.class, BigInteger.class);
  }

  @Override
  public BigInteger convert(final Integer integer4) throws ValueCodecException {
    return BigInteger.valueOf(integer4);
  }

}
