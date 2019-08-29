package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public final class IntegerCodec extends HBaseValueCodec<BigInteger> {

  private static final Charset CHARSET = Charset.forName("UTF-8");

  public IntegerCodec() {
    super(BigInteger.class, null, false);
  }

  @Override
  public Integer getEstimateByteLength(final BigInteger value) {
    return value.bitCount() / 8;
  }

  @Override
  public byte[] encode(final BigInteger value) {
    return toString(value).getBytes(CHARSET);
  }

  @Override
  public void encode(final BigInteger value, final ByteBuffer byteBuffer) {
    final ByteBuffer buffer = CHARSET.encode(toString(value));
    buffer.position(0);
    byteBuffer.put(buffer);
  }

  @Override
  public BigInteger decode(final byte[] bytes) {
    return toValue(new String(bytes, CHARSET));
  }

  @Override
  public BigInteger decode(final byte[] bytes, final int offset, final int length) {
    return toValue(new String(bytes, offset, length, CHARSET));
  }

  @Override
  public BigInteger decode(final ByteBuffer byteBuffer) {
    return toValue(CHARSET.decode(byteBuffer).toString());
  }

  private static String toString(final BigInteger value) {
    return value.toString(10);
  }

  private static BigInteger toValue(final String string) {
    return new BigInteger(string, 10);
  }

}
