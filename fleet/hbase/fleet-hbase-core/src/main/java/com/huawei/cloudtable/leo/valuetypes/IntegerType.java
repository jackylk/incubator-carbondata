package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;

import java.math.BigInteger;

public final class IntegerType extends ValueType<BigInteger>
    implements ValueType.Comparable<BigInteger>, ValueType.Numeric {

  public static final Identifier NAME = Identifier.of("INTEGER");

  public IntegerType() {
    super(NAME, BigInteger.class, null);
  }

  @Override
  public BigInteger getMinimum() {
    return null;
  }

  @Override
  public BigInteger getMaximum() {
    return null;
  }

  @Override
  public BigInteger getSimple() {
    return BigInteger.ZERO;
  }

}
