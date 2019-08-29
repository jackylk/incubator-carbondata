package com.huawei.cloudtable.leo.hbase.codecs;

import com.huawei.cloudtable.leo.HBaseValueCodec;
import com.huawei.cloudtable.leo.value.Bytes;

import java.nio.ByteBuffer;

public final class BytesCodec extends HBaseValueCodec<Bytes> {

  public BytesCodec() {
    super(Bytes.class, null, true);
  }

  @Override
  public Integer getEstimateByteLength(final Bytes value) {
    return value.getLength();
  }

  @Override
  public byte[] encode(final Bytes value) {
    // 该方法性能较差，不应该经常调
    return value.get();
  }

  @Override
  public void encode(final Bytes value, final ByteBuffer byteBuffer) {
    value.write(byteBuffer);
  }

  @Override
  public Bytes decode(final byte[] value) {
    return Bytes.of(value);
  }

  @Override
  public Bytes decode(final byte[] bytes, final int offset, final int length) {
    return Bytes.of(bytes, offset, length);
  }

  @Override
  public Bytes decode(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
    byteBuffer.get(bytes);
    return Bytes.of(bytes);
  }

}
