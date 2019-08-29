package com.huawei.cloudtable.leo.valuetypes;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.value.Bytes;

public final class BytesType extends ValueType<Bytes>
    implements ValueType.Comparable<Bytes> {

  public static final Identifier NAME = Identifier.of("BYTES");

  private static final byte[] EMPTY = new byte[0];

  public BytesType() {
    super(NAME, Bytes.class, null);
  }

  @Override
  public Bytes getMinimum() {
    return null;
  }

  @Override
  public Bytes getMaximum() {
    return null;
  }

  @Override
  public Bytes getSimple() {
    return Bytes.of(EMPTY);
  }

}
