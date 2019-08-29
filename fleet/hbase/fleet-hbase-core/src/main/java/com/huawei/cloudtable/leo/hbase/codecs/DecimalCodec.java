package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public final class DecimalCodec extends HBaseValueCodec<BigDecimal> {

  private static final Charset CHARSET = Charset.forName("UTF-8");

  public DecimalCodec() {
    super(BigDecimal.class, null, false);
  }

  @Override
  public Integer getEstimateByteLength(final BigDecimal value) {
    return value.precision() + value.scale(); // TODO ?
  }

  @Override
  public byte[] encode(final BigDecimal value) {
    return toString(value).getBytes(CHARSET);
  }

  @Override
  public void encode(final BigDecimal value, final ByteBuffer byteBuffer) {
    CHARSET.encode(toString(value));
  }

  @Override
  public BigDecimal decode(final byte[] bytes) {
    return toValue(new String(bytes, CHARSET));
  }

  @Override
  public BigDecimal decode(final byte[] bytes, final int offset, final int length) {
    return toValue(new String(bytes, offset, length, CHARSET));
  }

  @Override
  public BigDecimal decode(final ByteBuffer byteBuffer) {
    return toValue(CHARSET.decode(byteBuffer).toString());
  }

  private static String toString(final BigDecimal value) {
    return value.toPlainString();
  }

  private static BigDecimal toValue(final String string) {
    return new BigDecimal(string);
  }

}
