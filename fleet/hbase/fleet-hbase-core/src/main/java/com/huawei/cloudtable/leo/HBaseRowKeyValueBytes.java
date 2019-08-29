package com.huawei.cloudtable.leo;

public final class HBaseRowKeyValueBytes extends ValueBytes {

  private byte[] bytes;

  private int offset;

  private int length;

  @Override
  public byte[] getBytes() {
    return this.bytes;
  }

  @Override
  public int getOffset() {
    return this.offset;
  }

  @Override
  public int getLength() {
    return this.length;
  }

  void set(final byte[] bytes, final int offset, final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

}
