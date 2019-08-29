package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class BooleanCodec extends HBaseValueCodec<Boolean> {

  private static final int FIXED_LENGTH = 1;

  private static final byte TRUE = (byte) 0xFF;

  private static final byte FALSE = 0x00;

  public BooleanCodec() {
    super(Boolean.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Boolean value) {
    return new byte[]{value ? TRUE : FALSE};
  }

  @Override
  public void encode(final Boolean value, final ByteBuffer byteBuffer) {
    byteBuffer.put(value ? TRUE : FALSE);
  }

  @Override
  public Boolean decode(final byte[] bytes) {
    switch (bytes[0]) {
      case TRUE:
        return true;
      case FALSE:
        return false;
      default:
        throw new RuntimeException("Boolean should be 0x00 or 0xFF.");
    }
  }

  @Override
  public Boolean decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    switch (bytes[offset]) {
      case TRUE:
        return true;
      case FALSE:
        return false;
      default:
        throw new RuntimeException("Boolean should be 0x00 or 0xFF.");
    }
  }

  @Override
  public Boolean decode(final ByteBuffer byteBuffer) {
    switch (byteBuffer.get()) {
      case TRUE:
        return true;
      case FALSE:
        return false;
      default:
        throw new RuntimeException("Boolean should be 0x00 or 0xFF.");
    }
  }

}
