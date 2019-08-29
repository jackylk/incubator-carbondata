package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Integer2Codec extends HBaseValueCodec<Short> {

  private static final int FIXED_LENGTH = 2;

  public Integer2Codec() {
    super(Short.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Short value) {
    final short flippedValue = flipSignBit(value);
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (flippedValue >> 8);
    bytes[1] = (byte) flippedValue;
    return bytes;
  }

  @Override
  public void encode(final Short value, final ByteBuffer byteBuffer) {
    byteBuffer.putShort(flipSignBit(value));
  }

  @Override
  public Short decode(final byte[] bytes) {
    return flipSignBit((short)((bytes[0] << 8) + (bytes[1] & 0xFF)));
  }

  @Override
  public Short decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    return flipSignBit((short)((bytes[offset] << 8) + (bytes[offset + 1] & 0xFF)));
  }

  @Override
  public Short decode(final ByteBuffer byteBuffer) {
    return flipSignBit(byteBuffer.getShort());
  }

  private static short flipSignBit(final short value) {
    return (short)(value ^ 0x8000);
  }

}
