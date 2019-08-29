package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;

import java.nio.ByteBuffer;

public abstract class Aggregator<TResult> {

  public abstract TResult getResult();

  // TODO 聚合次数，超出最大限制时，抛异常
  public abstract void aggregate(final ParametersIterator parameters) throws EvaluateException;

  public interface Partible<TPartition extends Partition> {

    void aggregate(TPartition partition);

    TPartition newPartition();

    default TPartition newPartition(final ByteBuffer byteBuffer) {
      final TPartition partition = newPartition();
      partition.deserialize(byteBuffer);
      return partition;
    }

  }

  public static abstract class Partition {

    // TODO 聚合次数，超出最大限制时，抛异常
    public abstract void aggregate(final ParametersIterator parameters) throws EvaluateException;

    public abstract void serialize(ByteBuffer byteBuffer);

    public abstract void deserialize(ByteBuffer byteBuffer);

  }

  public static abstract class ParametersIterator implements java.util.Iterator {

    public abstract boolean nextIsNull();

    public abstract ValueBytes next(ValueCodec valueCodec);

    public <TElement> TElement next(final Class<TElement> clazz) {
      return clazz.cast(this.next());
    }

  }

}
