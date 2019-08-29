package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Integer4Codec extends HBaseValueCodec<Integer> {

  private static final int FIXED_LENGTH = 4;

  public Integer4Codec() {
    super(Integer.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Integer value) {
    final int flippedValue = flipSignBit(value);
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (flippedValue >> 24);
    bytes[1] = (byte) (flippedValue >> 16);
    bytes[2] = (byte) (flippedValue >> 8);
    bytes[3] = (byte) flippedValue;
    return bytes;
  }

  @Override
  public void encode(final Integer value, final ByteBuffer byteBuffer) {
    byteBuffer.putInt(flipSignBit(value));
  }

  @Override
  public Integer decode(final byte[] bytes) {
    int value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return flipSignBit(value);
  }

  @Override
  public Integer decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    int value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return flipSignBit(value);
  }

  @Override
  public Integer decode(final ByteBuffer byteBuffer) {
    return flipSignBit(byteBuffer.getInt());
  }

  private static int flipSignBit(final int value) {
    return value ^ 0x80000000;
  }

}
