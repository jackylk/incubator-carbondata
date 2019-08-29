package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.hbase.codecs.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseValueCodecManager extends ValueCodecManager {

  public static final HBaseValueCodecManager BUILD_IN;

  static {
    final List<HBaseValueCodec<?>> buildInValueCodecList = new ArrayList<>();
    buildInValueCodecList.add(new BooleanCodec());
    buildInValueCodecList.add(new BytesCodec());
    buildInValueCodecList.add(new DateCodec());
    buildInValueCodecList.add(new Decimal4Codec());
    buildInValueCodecList.add(new Decimal8Codec());
    buildInValueCodecList.add(new DecimalCodec());
    buildInValueCodecList.add(new Integer1Codec());
    buildInValueCodecList.add(new Integer2Codec());
    buildInValueCodecList.add(new Integer4Codec());
    buildInValueCodecList.add(new Integer8Codec());
    buildInValueCodecList.add(new IntegerCodec());
    buildInValueCodecList.add(new StringCodec.StringGB2312Codec());
    buildInValueCodecList.add(new StringCodec.StringGBKCodec());
    buildInValueCodecList.add(new StringCodec.StringUTF8Codec());
    buildInValueCodecList.add(new TimeCodec());
    buildInValueCodecList.add(new TimestampCodec());
    BUILD_IN = new HBaseValueCodecManager(buildInValueCodecList);
  }

  public HBaseValueCodecManager(final List<HBaseValueCodec<?>> valueCodecList) {
    final Map<Class, HBaseValueCodec> valueCodecMapByClass = new HashMap<>(valueCodecList.size());
    final Map<Class, Map<Charset, HBaseValueCodec>> valueCodecMapByClassAndCharset = new HashMap<>();
    if (valueCodecList.size() > Short.MAX_VALUE) {
      // TODO
      throw new UnsupportedOperationException();
    }
    for (int index = 0; index < valueCodecList.size(); index++) {
      final HBaseValueCodec valueCodec = valueCodecList.get(index);
      if (valueCodec.isDefault()) {
        if (valueCodecMapByClass.containsKey(valueCodec.getValueClass())) {
          // TODO
          throw new UnsupportedOperationException();
        }
        valueCodecMapByClass.put(valueCodec.getValueClass(), valueCodec);
      }
      if (valueCodec.getCharset() != null) {
        Map<Charset, HBaseValueCodec> valueCodecMapByCharset = valueCodecMapByClassAndCharset.get(valueCodec.getValueClass());
        if (valueCodecMapByCharset == null) {
          valueCodecMapByCharset = new HashMap<>();
          valueCodecMapByClassAndCharset.put(valueCodec.getValueClass(), valueCodecMapByCharset);
        } else {
          if (valueCodecMapByCharset.containsKey(valueCodec.getCharset())) {
            // TODO
            throw new UnsupportedOperationException();
          }
        }
        valueCodecMapByCharset.put(valueCodec.getCharset(), valueCodec);
      }
      valueCodec.index = (short) index;
    }
    this.valueCodecList = valueCodecList;
    this.valueCodecMapByClass = valueCodecMapByClass;
    this.valueCodecMapByClassAndCharset = valueCodecMapByClassAndCharset;
  }

  private final List<HBaseValueCodec<?>> valueCodecList;

  private final Map<Class, HBaseValueCodec> valueCodecMapByClass;

  private final Map<Class, Map<Charset, HBaseValueCodec>> valueCodecMapByClassAndCharset;

  @SuppressWarnings("unchecked")
  @Override
  public <TValue> HBaseValueCodec<TValue> getValueCodec(final Class<TValue> valueClass) {
    return this.valueCodecMapByClass.get(valueClass);
  }

  @Override
  public <TValue> HBaseValueCodec<TValue> getValueCodec(final Class<TValue> valueClass, final Charset charset) {
    final Map<Charset, HBaseValueCodec> valueCodecMapByCharset = this.valueCodecMapByClassAndCharset.get(valueClass);
    return valueCodecMapByCharset == null ? null : valueCodecMapByCharset.get(charset);
  }

  @Override
  public <TValue> HBaseValueCodec<TValue> getValueCodec(final ValueType<TValue> valueType) {
    return this.getValueCodec(valueType.getClazz());
  }

  @Override
  public <TValue> HBaseValueCodec<TValue> getValueCodec(final ValueType<TValue> valueType, final Charset charset) {
    return this.getValueCodec(valueType.getClazz(), charset);
  }

  public HBaseValueCodec getValueCode(final int index) {
    return this.valueCodecList.get(index);
  }

}
