package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.expression.SQLConstant;
import com.huawei.cloudtable.leo.language.statement.SQLValueType;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.value.Bytes;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public enum ColumnType {

  CHAR(Types.CHAR, String.class, 30) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      if (sourceColumnValueType.getParameterList().size() != 1) {
        throw new SyntaxException(sourceColumnValueType.getPosition());
      }
      return Collections.singletonMap(
          TableDefinition.ColumnProperty.FIXED_LENGTH,
          Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0)) // TODO 要转成字节数
      );
    }
  },

  VARCHAR(Types.VARCHAR, String.class, 30) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      if (sourceColumnValueType.getParameterList() == null) {
        return null;
      }
      if (sourceColumnValueType.getParameterList().size() != 1) {
        throw new SyntaxException(sourceColumnValueType.getPosition());
      }
      return Collections.singletonMap(
          TableDefinition.ColumnProperty.MAXIMUM_LENGTH,
          Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0)) // TODO 要转成字节数
      );
    }
  },

  NCHAR(Types.NCHAR, String.class, 30) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      if (sourceColumnValueType.getParameterList().size() != 1) {
        throw new SyntaxException(sourceColumnValueType.getPosition());
      }
      final Map<Identifier, String> properties = new HashMap<>(2);
      properties.put(
          TableDefinition.ColumnProperty.CHARSET,
          "UNICODE"// TODO 定义为常量
      );
      properties.put(
          TableDefinition.ColumnProperty.FIXED_LENGTH,
          Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0)) // TODO 要转成字节数
      );
      return properties;
    }
  },

  NVARCHAR(Types.NVARCHAR, String.class, 30) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      final Map<Identifier, String> properties = new HashMap<>(2);
      properties.put(
          TableDefinition.ColumnProperty.CHARSET,
          "UNICODE"// TODO 定义为常量
      );
      if (sourceColumnValueType.getParameterList() != null) {
        if (sourceColumnValueType.getParameterList().size() != 1) {
          throw new SyntaxException(sourceColumnValueType.getPosition());
        }
        properties.put(
            TableDefinition.ColumnProperty.MAXIMUM_LENGTH,
            Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0)) // TODO 要转成字节数
        );
      }
      return properties;
    }
  },

  BINARY(Types.BINARY, Bytes.class, 5) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      if (sourceColumnValueType.getParameterList().size() != 1) {
        throw new SyntaxException(sourceColumnValueType.getPosition());
      }
      return Collections.singletonMap(
          TableDefinition.ColumnProperty.FIXED_LENGTH,
          Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0))
      );
    }
  },

  VARBINARY(Types.VARBINARY, Bytes.class, 5) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
      if (sourceColumnValueType.getParameterList() == null) {
        return null;
      }
      if (sourceColumnValueType.getParameterList().size() != 1) {
        throw new SyntaxException(sourceColumnValueType.getPosition());
      }
      return Collections.singletonMap(
          TableDefinition.ColumnProperty.MAXIMUM_LENGTH,
          Integer.toString(ColumnType.getIntegerTypeParameter(sourceColumnValueType, 0))
      );
    }
  },

  BOOLEAN(Types.BOOLEAN, Boolean.class, 10),

  TINYINT(Types.TINYINT, Byte.class, 10),

  SMALLINT(Types.SMALLINT, Short.class, 10),

  INT(Types.INTEGER, Integer.class, 20),

  BIGINT(Types.BIGINT, Long.class, 20),

  INTEGER(Types.INTEGER, BigInteger.class, 20),

  FLOAT(Types.FLOAT, Float.class, 20) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) {
      // TODO
//      TableDefinition.ColumnProperty.PRECISION;
//      TableDefinition.ColumnProperty.SCALE;
      return null;
    }
  },

  REAL(Types.REAL, Double.class, 20) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) {
      // TODO
//      TableDefinition.ColumnProperty.PRECISION;
//      TableDefinition.ColumnProperty.SCALE;
      return null;
    }
  },

  DECIMAL(Types.DECIMAL, BigDecimal.class, 20) {
    @Override
    public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) {
      // TODO
//      TableDefinition.ColumnProperty.PRECISION;
//      TableDefinition.ColumnProperty.SCALE;
      return null;
    }
  },

  DATE(Types.DATE, Date.class, 20),

  TIME(Types.TIME, Time.class, 20),

  TIMESTAMP(Types.TIMESTAMP, Timestamp.class, 20);

  ColumnType(final int code, final Class<?> valueClass, final int defaultDisplaySize) {
    final ValueType<?> valueType = ValueTypeManager.BUILD_IN.getValueType(valueClass);
    if (valueType == null) {
      // TODO
      throw new UnsupportedOperationException();
    }
    this.code = code;
    this.name = Identifier.of(this.name());
    this.valueClass = valueClass;
    this.valueType = valueType;
    this.defaultDisplaySize = defaultDisplaySize;
  }

  private final int code;

  private final Identifier name;

  private final Class<?> valueClass;

  private final ValueType<?> valueType;

  private final int defaultDisplaySize;

  public int getCode() {
    return this.code;
  }

  public Identifier getName() {
    return this.name;
  }

  public Class<?> getValueClass() {
    return this.valueClass;
  }

  public ValueType<?> getValueType() {
    return this.valueType;
  }

  public int getDefaultDisplaySize() {
    return this.defaultDisplaySize;
  }

  public Map<Identifier, String> getProperties(final SQLValueType sourceColumnValueType) throws SyntaxException {
    return null;
  }

  public static ColumnType get(final Identifier typeName) {
    return COLUMN_TYPE_MAP_BY_NAME.get(typeName);
  }

  public static ColumnType get(final Class<?> valueClass) {
    return COLUMN_TYPE_MAP_BY_CLASS.get(valueClass);
  }

  public static ColumnType get(final TableDefinition.Column<?> column) {
    final String columnTypeProperty = column.getProperty(ColumnProperty.TYPE);
    if (columnTypeProperty == null) {
      return ColumnType.get(column.getValueClass());
    } else {
      final ColumnType columnType = ColumnType.get(Identifier.of(columnTypeProperty));
      if (columnType == null) {
        throw new RuntimeException();
      }
      return columnType;
    }
  }

  private static int getIntegerTypeParameter(final SQLValueType sourceColumnValueType, final int parameterIndex) throws SyntaxException {
    final SQLConstant sourceColumnValueTypeParameter = sourceColumnValueType.getParameterList().get(parameterIndex);
    if (sourceColumnValueTypeParameter == null) {
      throw new SyntaxException(sourceColumnValueType.getPosition());
    }
    if (!(sourceColumnValueTypeParameter instanceof SQLConstant.Integer)) {
      throw new SyntaxException(sourceColumnValueTypeParameter.getPosition());
    }
    final int parameterValue;
    try {
      parameterValue = Integer.valueOf(((SQLConstant.Integer)sourceColumnValueTypeParameter).getValue());
    } catch (NumberFormatException ignore) {
      throw new SyntaxException(sourceColumnValueTypeParameter.getPosition());
    }
    return parameterValue;
  }

  private static final Map<Identifier, ColumnType> COLUMN_TYPE_MAP_BY_NAME;

  private static final Map<Class<?>, ColumnType> COLUMN_TYPE_MAP_BY_CLASS;

  static {
    final Map<Identifier, ColumnType> columnTypeMapByName = new HashMap<>(ColumnType.values().length);
    for (ColumnType valueType : ColumnType.values()) {
      columnTypeMapByName.put(valueType.getName(), valueType);
    }

    final Map<Class<?>, ColumnType> columnTypeMapByClass = new HashMap<>();
    columnTypeMapByClass.put(String.class, VARCHAR);
    columnTypeMapByClass.put(Bytes.class, VARBINARY);
    columnTypeMapByClass.put(Boolean.class, BOOLEAN);
    columnTypeMapByClass.put(Byte.class, TINYINT);
    columnTypeMapByClass.put(Short.class, SMALLINT);
    columnTypeMapByClass.put(Integer.class, INT);
    columnTypeMapByClass.put(Long.class, BIGINT);
    columnTypeMapByClass.put(BigInteger.class, INTEGER);
    columnTypeMapByClass.put(Float.class, FLOAT);
    columnTypeMapByClass.put(Double.class, REAL);
    columnTypeMapByClass.put(BigDecimal.class, DECIMAL);
    columnTypeMapByClass.put(Date.class, DATE);
    columnTypeMapByClass.put(Time.class, TIME);
    columnTypeMapByClass.put(Timestamp.class, TIMESTAMP);

    COLUMN_TYPE_MAP_BY_NAME = columnTypeMapByName;
    COLUMN_TYPE_MAP_BY_CLASS = columnTypeMapByClass;
  }

}
