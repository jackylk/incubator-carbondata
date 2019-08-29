package com.huawei.cloudtable.leo;

public abstract class HBaseRowKeyValueIterator {

  public abstract boolean hasNext();

  public abstract void next();

  public abstract Object get();

  // TODO Bytes is immutable in the outer.
  public abstract ValueBytes getAsBytes();

}
