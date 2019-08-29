package com.huawei.cloudtable.leo;

public abstract class HBaseRowKeyCodec {

  public abstract byte[] encode(HBaseRowKeyValueIterator rowKeyValueIterator);

  public abstract byte[] encode(HBaseRowKeyValueIterator rowKeyValueIterator, int rowKeyValueCount);

  public abstract Decoder decoder();

  public static abstract class Decoder extends HBaseRowKeyValueIterator {

    public abstract void setRowKey(byte[] rowKey);

    public abstract void setRowKey(byte[] rowKey, int offset, int length);

  }

}
