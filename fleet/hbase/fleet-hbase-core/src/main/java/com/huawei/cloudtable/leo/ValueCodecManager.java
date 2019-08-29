package com.huawei.cloudtable.leo;

import java.nio.charset.Charset;

public abstract class ValueCodecManager {

  public abstract  <TValue> ValueCodec<TValue> getValueCodec(Class<TValue> valueClass);

  public abstract  <TValue> ValueCodec<TValue> getValueCodec(Class<TValue> valueClass, Charset charset);

  public abstract  <TValue> ValueCodec<TValue> getValueCodec(ValueType<TValue> valueType);

  public abstract  <TValue> ValueCodec<TValue> getValueCodec(ValueType<TValue> valueType, Charset charset);

}
