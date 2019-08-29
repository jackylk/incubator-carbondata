package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.common.BytesHelper;

import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class  ValueBytes implements Comparable<ValueBytes> {

  public static ValueBytes of(final byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("Variable [value] is null.");
    }
    return of0(value);
  }

  public static ValueBytes of(final byte[] value, final int offset, final int length) {
    if (value == null) {
      throw new IllegalArgumentException("Variable [value] is null.");
    }
    if (offset == 0 && length == value.length) {
      return of0(value);
    }
    // TODO check offset and length;
    return of0(value, offset, length);
  }

  private static ValueBytes of0(final byte[] value) {
    return new ValueBytes() {
      @Override
      public byte[] getBytes() {
        return value;
      }

      @Override
      public int getOffset() {
        return 0;
      }

      @Override
      public int getLength() {
        return value.length;
      }
    };
  }

  private static ValueBytes of0(final byte[] value, final int offset, final int length) {
    return new ValueBytes() {
      @Override
      public byte[] getBytes() {
        return value;
      }

      @Override
      public int getOffset() {
        return offset;
      }

      @Override
      public int getLength() {
        return length;
      }
    };
  }

  public static ValueBytes read(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
    byteBuffer.get(bytes);
    return ValueBytes.of(bytes);
  }

  public static ValueBytes readWithLength(final ByteBuffer byteBuffer) {
    final int originLimit = byteBuffer.limit();
    try {
      final int length = byteBuffer.getInt();
      byteBuffer.limit(byteBuffer.position() + length);
      return read(byteBuffer);
    } finally {
      byteBuffer.limit(originLimit);
    }
  }

  public final void write(final ByteBuffer byteBuffer) {
    byteBuffer.put(this.getBytes(), this.getOffset(), this.getLength());
  }

  public final void writeWithLength(final ByteBuffer byteBuffer) {
    final int lengthPosition = byteBuffer.position();
    byteBuffer.putInt(0);
    final int valuePosition = byteBuffer.position();
    byteBuffer.put(this.getBytes(), this.getOffset(), this.getLength());
    byteBuffer.putInt(lengthPosition, byteBuffer.position() - valuePosition);
  }

  public final <TValue> TValue decode(final ValueCodec<TValue> valueCodec) {
    return valueCodec.decode(this.getBytes(), this.getOffset(), this.getLength());
  }

  @Override
  public final int compareTo(final ValueBytes that) {
    return BytesHelper.compare(
        this.getBytes(),
        this.getOffset(),
        this.getLength(),
        that.getBytes(),
        that.getOffset(),
        that.getLength()
    );
  }

  public ValueBytes duplicate() {
    return ValueBytes.of(this.getBytes(), this.getOffset(), this.getLength());
  }

  public abstract byte[] getBytes();

  public abstract int getOffset();

  public abstract int getLength();

  @Override
  public String toString() {
    return Arrays.toString(Arrays.copyOfRange(this.getBytes(), this.getOffset(), this.getOffset() + this.getLength()));
  }

}
