package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.common.ExtensionCollector;

import java.util.*;

public class ValueTypeManager {

  public static final ValueTypeManager BUILD_IN;

  static {
    final Iterator<Class<? extends ValueType>> buildInValueTypeClasses = ExtensionCollector.getExtensionClasses(ValueType.class);
    final List<ValueType<?>> buildInValueTypeList = new ArrayList<>();
    while (buildInValueTypeClasses.hasNext()) {
      try {
        buildInValueTypeList.add(buildInValueTypeClasses.next().newInstance());
      } catch (InstantiationException | IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    }
    BUILD_IN = new ValueTypeManager(buildInValueTypeList);
  }

  private ValueTypeManager(final List<ValueType<?>> valueTypeList) {
    final Map<Identifier, ValueType<?>> valueTypeMapByName = new HashMap<>(valueTypeList.size());
    final Map<Class, ValueType<?>> valueTypeMapByClass = new HashMap<>(valueTypeList.size());
    for (ValueType<?> valueType : valueTypeList) {
      valueTypeMapByName.put(valueType.getName(), valueType);
      valueTypeMapByClass.put(valueType.getClazz(), valueType);
    }
    this.valueTypeMapByName = Collections.unmodifiableMap(valueTypeMapByName);
    this.valueTypeMapByClass = Collections.unmodifiableMap(valueTypeMapByClass);
  }

  private final Map<Identifier, ValueType<?>> valueTypeMapByName;

  private final Map<Class, ValueType<?>> valueTypeMapByClass;

  public final ValueType<?> getValueType(final Identifier valueTypeName) {
    return this.valueTypeMapByName.get(valueTypeName);
  }

  @SuppressWarnings("unchecked")
  public final <TValue> ValueType<TValue> getValueType(final Class<TValue> valueClass) {
    return (ValueType<TValue>)this.valueTypeMapByClass.get(valueClass);
  }

}
