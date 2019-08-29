package com.huawei.cloudtable.leo.valueconverters;

import com.huawei.cloudtable.leo.ValueCodecException;
import com.huawei.cloudtable.leo.ValueConverter;

import java.math.BigInteger;

public final class Integer1ToIntegerConverter extends ValueConverter<Byte, BigInteger> implements ValueConverter.Implicit {

  public Integer1ToIntegerConverter() {
    super(Byte.class, BigInteger.class);
  }

  @Override
  public BigInteger convert(final Byte integer1) throws ValueCodecException {
    return BigInteger.valueOf(integer1);
  }

}
