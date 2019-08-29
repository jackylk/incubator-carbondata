package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public abstract class StringCodec extends HBaseValueCodec<String> {

  public static final class StringUTF8Codec extends StringCodec {

    public StringUTF8Codec() {
      super(true, Charset.forName("UTF-8"), true);
    }

  }

  public static final class StringGBKCodec extends StringCodec {

    public StringGBKCodec() {
      super(true, Charset.forName("GBK"), false);
    }

  }

  public static final class StringGB2312Codec extends StringCodec {

    public StringGB2312Codec() {
      super(true, Charset.forName("GB2312"), false);
    }

  }

  private StringCodec(final boolean orderPreserving, final Charset charset, final boolean _default) {
    super(String.class, null, orderPreserving, charset, _default);
    this.averageBytesPerCharacter = this.charset.newEncoder().averageBytesPerChar();
  }

  private final ThreadLocal<CharsetEncoder> encoderCache = new ThreadLocal<CharsetEncoder>() {
    @Override
    protected CharsetEncoder initialValue() {
      return StringCodec.this.charset.newEncoder();
    }
  };

  private final float averageBytesPerCharacter;

  @Override
  public Integer getEstimateByteLength(final String value) {
    return Math.round(value.length() * this.averageBytesPerCharacter);
  }

  @Override
  public byte[] encode(final String value) {
    return value.getBytes(this.charset);
  }

  @Override
  public void encode(final String value, final ByteBuffer byteBuffer) {
    this.encoderCache.get().encode(CharBuffer.wrap(value), byteBuffer, true);// TODO 此处的CharBuffer.wrap()会导致较大的性能损失(15倍)，后续可考虑直接将String类型改成CharBuffer
  }

  @Override
  public String decode(final byte[] bytes) {
    return new String(bytes, this.charset);
  }

  @Override
  public String decode(final byte[] bytes, final int offset, final int length) {
    return new String(bytes, offset, length, this.charset);
  }

  @Override
  public String decode(final ByteBuffer byteBuffer) {
    return this.charset.decode(byteBuffer).toString();
  }

}
