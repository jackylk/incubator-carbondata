package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;

public abstract class DirectGet<TValue> extends Evaluation<TValue> {

  DirectGet(final Class<TValue> valueClass, final boolean nullable) {
    super(valueClass, nullable);
  }

  public abstract ValueBytes evaluate(final RuntimeContext context, final ValueCodec<TValue> valueCodec) throws EvaluateException;

}
