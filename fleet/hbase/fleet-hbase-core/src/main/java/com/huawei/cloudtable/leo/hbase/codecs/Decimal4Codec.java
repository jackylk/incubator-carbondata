package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Decimal4Codec extends HBaseValueCodec<Float> {

  private static final int FIXED_LENGTH = 4;

  public Decimal4Codec() {
    super(Float.class, FIXED_LENGTH, false);
  }

  @Override
  public byte[] encode(final Float value) {
    final int integer = toInteger4(value);
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (integer >> 24);
    bytes[1] = (byte) (integer >> 16);
    bytes[2] = (byte) (integer >> 8);
    bytes[3] = (byte) integer;
    return bytes;
  }

  @Override
  public void encode(final Float value, final ByteBuffer byteBuffer) {
    byteBuffer.putInt(toInteger4(value));
  }

  @Override
  public Float decode(final byte[] bytes) {
    int value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return toDecimal4(value);
  }

  @Override
  public Float decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    int value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return toDecimal4(value);
  }

  @Override
  public Float decode(final ByteBuffer byteBuffer) {
    return toDecimal4(byteBuffer.getInt());
  }

  private static int toInteger4(final float value) {
    return Float.floatToIntBits(value);
  }

  private static float toDecimal4(final int value) {
    return Float.intBitsToFloat(value);
  }

}
