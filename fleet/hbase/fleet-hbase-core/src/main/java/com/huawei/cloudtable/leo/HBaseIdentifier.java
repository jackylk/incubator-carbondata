package com.huawei.cloudtable.leo;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

public final class HBaseIdentifier extends Identifier {

  public static HBaseIdentifier of(final String string) {
    return new HBaseIdentifier(string);
  }

  public static HBaseIdentifier read(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
    byteBuffer.get(bytes);
    return new HBaseIdentifier(bytes);
  }

  public static HBaseIdentifier readWithLength(final ByteBuffer byteBuffer) {
    final int originLimit = byteBuffer.limit();
    try {
      final int length = byteBuffer.getInt();
      byteBuffer.limit(byteBuffer.position() + length);
      return read(byteBuffer);
    } finally {
      byteBuffer.limit(originLimit);
    }
  }

  private HBaseIdentifier(final String string) {
    super(string);
  }

  private HBaseIdentifier(final byte[] bytes) {
    super(Bytes.toString(bytes));
    this.bytes = bytes;
  }

  private byte[] bytes;

  public void write(final ByteBuffer byteBuffer) {
    final byte[] bytes = this.getBytes();
    byteBuffer.put(bytes, 0, bytes.length);
  }

  public void writeWithLength(final ByteBuffer byteBuffer) {
    final byte[] bytes = this.getBytes();
    final int lengthPosition = byteBuffer.position();
    byteBuffer.putInt(0);
    final int valuePosition = byteBuffer.position();
    byteBuffer.put(bytes, 0, bytes.length);
    byteBuffer.putInt(lengthPosition, byteBuffer.position() - valuePosition);
  }

  byte[] getBytes() {
    if (this.bytes == null) {
      this.bytes = Bytes.toBytes(this.string);
    }
    return this.bytes;
  }

}
