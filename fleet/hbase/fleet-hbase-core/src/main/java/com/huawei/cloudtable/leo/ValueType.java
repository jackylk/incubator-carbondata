package com.huawei.cloudtable.leo;

// TODO 通过配置文件建立DataType与java.sql.Types的关系。

/**
 * 注册的数据类型，不能是原始类，接口类，注释类或者抽象类，不能与已注册的其它数据类型形成继承关系。
 * 用户自定义的数据类型类不能放在org.apache.lemon.包下。
 */
public abstract class ValueType<TValue> {

  /**
   * 字符数15
   */
  public static final byte NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

  public static final String NAME_PATTERN = "[a-zA-Z_$][a-zA-Z0-9_$]{0," + NAME_MAXIMUM_LENGTH + "}";

  /**
   * 字节数
   */
  public static final short DESCRIPTION_MAXIMUM_LENGTH = Short.MAX_VALUE;

  public static final String DESCRIPTION_PATTERN = "[\\W]{0," + DESCRIPTION_MAXIMUM_LENGTH + "}";

  public ValueType(final Identifier name, final Class<TValue> clazz, final String description) {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    if (!name.matches(NAME_PATTERN)) {
      throw new IllegalArgumentException("Argument [name] is not matches " + NAME_PATTERN + ".");
    }
    if (clazz == null) {
      throw new IllegalArgumentException("Argument [clazz] is null.");
    }
    if (clazz.isPrimitive()) {
      throw new IllegalArgumentException("Argument [clazz] is primitive class.");
    }
    if (clazz.isInterface()) {
      throw new IllegalArgumentException("Argument [clazz] is an interface.");
    }
// TODO
//    if (Modifier.isAbstract(clazz.getModifiers())) {
//      throw new IllegalArgumentException("Argument [clazz] is an abstract class.");
//    }
    if (description != null && !description.matches(DESCRIPTION_PATTERN)) {
      throw new IllegalArgumentException("Argument [description] is not matches " + DESCRIPTION_PATTERN + ".");
    }
    this.name = name;
    this.clazz = clazz;
    this.description = description;
  }

  private final Identifier name;

  private final Class<TValue> clazz;

  private final String description;

  public Identifier getName() {
    return name;
  }

  public final Class<TValue> getClazz() {
    return this.clazz;
  }

  public String getDescription() {
    return this.description;
  }

  public abstract TValue getSimple();

  public interface Comparable<TValue extends java.lang.Comparable<? super TValue>> {

    TValue getMinimum();

    TValue getMaximum();

  }

  public interface Numeric {

    // 精度，小数位转换

  }

  public interface Characters {

  }

}
