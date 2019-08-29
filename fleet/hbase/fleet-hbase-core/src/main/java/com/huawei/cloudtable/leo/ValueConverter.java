package com.huawei.cloudtable.leo;

/**
 * 定义的隐式转换链图必须为有向无环图。
 * 注册隐式转换器前，要先注册原始数据类型和目标数据类型。
 */
public abstract class ValueConverter<TSource, TTarget> {

  public ValueConverter(final Class<TSource> sourceClass, final Class<TTarget> targetClass) {
    if (sourceClass == null) {
      throw new IllegalArgumentException("Argument [sourceClass] is null.");
    }
    if (targetClass == null) {
      throw new IllegalArgumentException("Argument [targetClass] is null.");
    }
    if (targetClass == Object.class) {
      throw new IllegalArgumentException("Argument [targetClass] is " + Object.class.getName() + ".");
    }
    this.sourceClass = sourceClass;
    this.targetClass = targetClass;
  }

  private final Class<TSource> sourceClass;

  private final Class<TTarget> targetClass;

  public Class<TSource> getSourceClass() {
    return this.sourceClass;
  }

  public Class<TTarget> getTargetClass() {
    return this.targetClass;
  }

  public abstract TTarget convert(TSource source);

  @Override
  public String toString() {
    return this.sourceClass.getName() + " -> " + this.targetClass.getName();
  }

  public interface Implicit {

    // nothing.

  }

}
