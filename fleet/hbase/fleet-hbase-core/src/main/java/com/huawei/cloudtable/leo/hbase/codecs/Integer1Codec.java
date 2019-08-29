package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;

public final class Integer1Codec extends HBaseValueCodec<Byte> {

  private static final int FIXED_LENGTH = 1;

  public Integer1Codec() {
    super(Byte.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Byte value) {
    return new byte[]{flipSignBit(value)};
  }

  @Override
  public void encode(final Byte value, final ByteBuffer byteBuffer) {
    byteBuffer.put(flipSignBit(value));
  }

  @Override
  public Byte decode(final byte[] bytes) {
    return flipSignBit(bytes[0]);
  }

  @Override
  public Byte decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    return flipSignBit(bytes[offset]);
  }

  @Override
  public Byte decode(final ByteBuffer byteBuffer) {
    return flipSignBit(byteBuffer.get());
  }

  private static byte flipSignBit(final byte value) {
    return (byte)(value ^ 0x80);
  }

}
