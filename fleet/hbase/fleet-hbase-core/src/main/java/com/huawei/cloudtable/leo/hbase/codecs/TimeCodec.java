package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;
import com.huawei.cloudtable.leo.value.Time;

import java.nio.ByteBuffer;

public final class TimeCodec extends HBaseValueCodec<Time> {

  private static final int FIXED_LENGTH = 4;

  public TimeCodec() {
    super(Time.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Time value) {
    final int flippedTime = flipSignBit((int)value.getTime());
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (flippedTime >> 24);
    bytes[1] = (byte) (flippedTime >> 16);
    bytes[2] = (byte) (flippedTime >> 8);
    bytes[3] = (byte) flippedTime;
    return bytes;
  }

  @Override
  public void encode(final Time value, final ByteBuffer byteBuffer) {
    byteBuffer.putInt(flipSignBit((int)value.getTime()));
  }

  @Override
  public Time decode(final byte[] bytes) {
    int value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return Time.of(flipSignBit(value));
  }

  @Override
  public Time decode(byte[] bytes, int offset, int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    int value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return Time.of(flipSignBit(value));
  }

  @Override
  public Time decode(final ByteBuffer byteBuffer) {
    return  Time.of(flipSignBit(byteBuffer.getShort()));
  }

  private static int flipSignBit(final int value) {
    return value ^ 0x80000000;
  }

}
