package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigInteger;

public final class Integer2ToIntegerConverter extends ValueConverter<Short, BigInteger> implements ValueConverter.Implicit {

  public Integer2ToIntegerConverter() {
    super(Short.class, BigInteger.class);
  }

  @Override
  public BigInteger convert(final Short integer2) throws ValueCodecException {
    return BigInteger.valueOf(integer2);
  }

}
