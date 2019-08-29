package com.huawei.cloudtable.leo;

import java.nio.charset.Charset;

public abstract class HBaseValueCodec<TValue> extends ValueCodec<TValue> {

  public HBaseValueCodec(
      final Class<TValue> valueClass,
      final Integer fixedLength,
      final boolean orderPreserving
  ) {
    super(valueClass, fixedLength, orderPreserving);
  }

  public HBaseValueCodec(
      final Class<TValue> valueClass,
      final Integer fixedLength,
      final boolean orderPreserving,
      final Charset charset,
      final boolean _default
  ) {
    super(valueClass, fixedLength, orderPreserving, charset, _default);
  }

  Short index;

}
