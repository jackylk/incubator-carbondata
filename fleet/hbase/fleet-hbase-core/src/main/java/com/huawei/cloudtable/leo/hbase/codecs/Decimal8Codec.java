package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Decimal8Codec extends HBaseValueCodec<Double> {

  private static final int FIXED_LENGTH = 8;

  public Decimal8Codec() {
    super(Double.class, FIXED_LENGTH, false);
  }
  
  @Override
  public byte[] encode(final Double value) {
    final long integer = toInteger8(value);
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (integer >> 56);
    bytes[1] = (byte) (integer >> 48);
    bytes[2] = (byte) (integer >> 40);
    bytes[3] = (byte) (integer >> 32);
    bytes[4] = (byte) (integer >> 24);
    bytes[5] = (byte) (integer >> 16);
    bytes[6] = (byte) (integer >> 8);
    bytes[7] = (byte) integer;
    return bytes;
  }

  @Override
  public void encode(final Double value, final ByteBuffer byteBuffer) {
    byteBuffer.putLong(toInteger8(value));
  }

  @Override
  public Double decode(final byte[] bytes) {
    long value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return toDecimal8(value);
  }

  @Override
  public Double decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    long value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return toDecimal8(value);
  }

  @Override
  public Double decode(final ByteBuffer byteBuffer) {
    return toDecimal8(byteBuffer.getLong());
  }

  private static long toInteger8(final double value) {
    return Double.doubleToLongBits(value);
  }

  private static double toDecimal8(final long value) {
    return Double.longBitsToDouble(value);
  }
  
}
