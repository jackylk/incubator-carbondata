package com.huawei.cloudtable.leo.value;

import com.huawei.cloudtable.leo.common.BytesHelper;

import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class Bytes implements Comparable<Bytes> {

  public static Bytes of(final byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("Variable [value] is null.");
    }
    return of0(value);
  }

  public static Bytes of(final byte[] value, final int offset, final int length) {
    if (value == null) {
      throw new IllegalArgumentException("Variable [value] is null.");
    }
    if (offset == 0 && length == value.length) {
      return of0(value);
    }
    // TODO check offset and length;
    return of0(value, offset, length);
  }

  private static Bytes of0(final byte[] value) {
    return new Bytes() {
      @Override
      public int getOffset() {
        return 0;
      }

      @Override
      public int getLength() {
        return value.length;
      }

      @Override
      public byte[] get() {
        return value.clone();
      }

      @Override
      public byte get(int index) {
        return value[index];
      }

      @Override
      byte[] getBytes() {
        return value;
      }
    };
  }

  private static Bytes of0(final byte[] value, final int offset, final int length) {
    return new Bytes() {
      @Override
      public int getOffset() {
        return offset;
      }

      @Override
      public int getLength() {
        return length;
      }

      @Override
      public byte[] get() {
        return Arrays.copyOfRange(value, offset, offset + length);
      }

      @Override
      public byte get(int index) {
        return value[offset + index];
      }

      @Override
      byte[] getBytes() {
        return value;
      }
    };
  }

  public static Bytes read(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
    byteBuffer.get(bytes);
    return Bytes.of0(bytes);
  }

  public static Bytes readWithLength(final ByteBuffer byteBuffer) {
    final int originLimit = byteBuffer.limit();
    try {
      final int length = byteBuffer.getInt();
      byteBuffer.limit(byteBuffer.position() + length);
      return read(byteBuffer);
    } finally {
      byteBuffer.limit(originLimit);
    }
  }

  private Bytes() {
    // to do nothing.
  }

  public abstract int getOffset();

  public abstract int getLength();

  public abstract byte[] get();

  public abstract byte get(int index);

  abstract byte[] getBytes();

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

  @Override
  public final int compareTo(final Bytes that) {
    return BytesHelper.compare(
        this.getBytes(),
        this.getOffset(),
        this.getLength(),
        that.getBytes(),
        that.getOffset(),
        that.getLength()
    );
  }

}
