package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Integer8Codec extends HBaseValueCodec<Long> {

  private static final int FIXED_LENGTH = 8;

  public Integer8Codec() {
    super(Long.class, FIXED_LENGTH, true);
  }
  
  @Override
  public byte[] encode(final Long value) {
    final long flippedValue = flipSignBit(value);
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (flippedValue >> 56);
    bytes[1] = (byte) (flippedValue >> 48);
    bytes[2] = (byte) (flippedValue >> 40);
    bytes[3] = (byte) (flippedValue >> 32);
    bytes[4] = (byte) (flippedValue >> 24);
    bytes[5] = (byte) (flippedValue >> 16);
    bytes[6] = (byte) (flippedValue >> 8);
    bytes[7] = (byte) flippedValue;
    return bytes;
  }

  @Override
  public void encode(final Long value, final ByteBuffer byteBuffer) {
    byteBuffer.putLong(flipSignBit(value));
  }

  @Override
  public Long decode(final byte[] bytes) {
    long value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return flipSignBit(value);
  }

  @Override
  public Long decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    long value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return flipSignBit(value);
  }

  @Override
  public Long decode(final ByteBuffer byteBuffer) {
    return flipSignBit(byteBuffer.getLong());
  }

  private static long flipSignBit(final long value) {
    return value ^ 0x8000000000000000L;
  }
  
}
