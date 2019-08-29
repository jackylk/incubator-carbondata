package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;
import com.huawei.cloudtable.leo.value.Timestamp;

import java.nio.ByteBuffer;

public final class TimestampCodec extends HBaseValueCodec<Timestamp> {

  private static final int FIXED_LENGTH = 8;

  public TimestampCodec() {
    super(Timestamp.class, FIXED_LENGTH, true);
  }

  @Override
  public byte[] encode(final Timestamp value) {
    final long flippedTime = flipSignBit(value.getTime());
    final byte[] bytes = new byte[FIXED_LENGTH];
    bytes[0] = (byte) (flippedTime >> 56);
    bytes[1] = (byte) (flippedTime >> 48);
    bytes[2] = (byte) (flippedTime >> 40);
    bytes[3] = (byte) (flippedTime >> 32);
    bytes[4] = (byte) (flippedTime >> 24);
    bytes[5] = (byte) (flippedTime >> 16);
    bytes[6] = (byte) (flippedTime >> 8);
    bytes[7] = (byte) flippedTime;
    return bytes;
  }

  @Override
  public void encode(final Timestamp value, final ByteBuffer byteBuffer) {
    byteBuffer.putLong(flipSignBit(value.getTime()));
  }

  @Override
  public Timestamp decode(final byte[] bytes) {
    long value = bytes[0];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[i] & 0xFF);
    }
    return Timestamp.valueOf(flipSignBit(value));
  }

  @Override
  public Timestamp decode(final byte[] bytes, final int offset, final int length) {
    if (length != FIXED_LENGTH) {
      throw new IllegalArgumentException();
    }
    long value = bytes[offset];
    for (int i = 1; i < FIXED_LENGTH; i++) {
      value = (value << 8) + (bytes[offset + i] & 0xFF);
    }
    return Timestamp.valueOf(flipSignBit(value));
  }

  @Override
  public Timestamp decode(final ByteBuffer byteBuffer) {
    return Timestamp.valueOf(flipSignBit(byteBuffer.getLong()));
  }

  private static long flipSignBit(final long value) {
    return value ^ 0x8000000000000000L;
  }

}
