package com.huawei.cloudtable.leo;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class ValueCodec<TValue> {

  public ValueCodec(
      final Class<TValue> valueClass,
      final Integer fixedLength,
      final boolean orderPreserving
  ) {
    if (valueClass == null) {
      throw new IllegalArgumentException("Variable [valueClass] is null.");
    }
    if (!Comparable.class.isAssignableFrom(valueClass) && orderPreserving) {
      // TODO
      throw new UnsupportedOperationException();
    }
    this.valueClass = valueClass;
    this.fixedLength = fixedLength;
    this.orderPreserving = orderPreserving;
    this.charset = null;
    this._default = true;
  }

  public ValueCodec(
      final Class<TValue> valueClass,
      final Integer fixedLength,
      final boolean orderPreserving,
      final Charset charset,
      final boolean _default
  ) {
    if (valueClass == null) {
      throw new IllegalArgumentException("Variable [valueClass] is null.");
    }
    if (!Comparable.class.isAssignableFrom(valueClass) && orderPreserving) {
      // TODO
      throw new UnsupportedOperationException();
    }
    if (charset == null) {
      throw new IllegalArgumentException("Variable [charset] is null.");
    }
    this.valueClass = valueClass;
    this.fixedLength = fixedLength;
    this.orderPreserving = orderPreserving;
    this.charset = charset;
    this._default = _default;
  }

  private final Class<TValue> valueClass;

  private final Integer fixedLength;

  private final boolean orderPreserving;

  protected final Charset charset;

  private final boolean _default;

  public final Class<TValue> getValueClass() {
    return this.valueClass;
  }

  /**
   * @return return <code>null</code> means is not fixed getLength type.
   */
  public final Integer getFixedLength() {
    return this.fixedLength;
  }

  public final Charset getCharset() {
    return this.charset;
  }

  /**
   * @return return <code>null</code> means unknown.
   */
  public Integer getEstimateByteLength(final TValue data) {
    return this.fixedLength;
  }

  public final boolean isFixedLength() {
    return this.fixedLength != null;
  }

  /**
   * Indicates whether this instance writes encoded byte[] 's which preserve the natural sort order build the unencoded clazz.
   */
  public final boolean isOrderPreserving() {
    return this.orderPreserving;
  }

  public final boolean isDefault() {
    return this._default;
  }

  public abstract byte[] encode(TValue value);

  /**
   * @throws BufferOverflowException Overflow.
   */
  public abstract void encode(TValue value, ByteBuffer byteBuffer);

  public final void encodeWithLength(final TValue value, final ByteBuffer byteBuffer) {
    final int lengthPosition = byteBuffer.position();
    byteBuffer.putInt(0);
    final int dataPosition = byteBuffer.position();
    this.encode(value, byteBuffer);
    byteBuffer.putInt(lengthPosition, byteBuffer.position() - dataPosition);
  }

  /**
   * @throws BufferUnderflowException Overflow.
   */
  public abstract TValue decode(byte[] bytes);

  /**
   * @throws BufferUnderflowException Overflow.
   */
  public abstract TValue decode(byte[] bytes, int offset, int length);

  /**
   * @throws BufferUnderflowException Overflow.
   */
  public abstract TValue decode(ByteBuffer byteBuffer);

  public final TValue decodeWithLength(final ByteBuffer byteBuffer) {
    final int originLimit = byteBuffer.limit();
    try {
      final int length = byteBuffer.getInt();
      byteBuffer.limit(byteBuffer.position() + length);
      return this.decode(byteBuffer);
    } finally {
      byteBuffer.limit(originLimit);
    }
  }

}
