package com.huawei.cloudtable.leo;

import java.math.BigDecimal;
import java.util.Locale;

import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;

public class SparkHBaseTypeConverter {
  public static Class<?> toValueClass(String valueType) {
    valueType = valueType.toUpperCase(Locale.ROOT);
    switch (valueType) {
      case "CHAR":
      case "VARCHAR":
      case "NVARCHAR":
      case "STRING":
        return String.class;
      case "TINYINT":
        return Byte.class;
      case "SMALLINT":
        return Short.class;
      case "INT":
        return Integer.class;
      case "BIGINT":
        return Long.class;
      case "LONG":
        return Long.class;
      case "DECIMAL":
        return BigDecimal.class;
      case "FLOAT":
        return Float.class;
      case "SHORT":
        return Short.class;
      case "REAL":
        return Double.class;
      case "DOUBLE":
        return Double.class;
      case "DATE":
        return Date.class;
      case "TIME":
        return Time.class;
      case "TIMESTAMP":
        return Timestamp.class;
      default:
        throw new RuntimeException("Unsupported value type. " + valueType);
    }
  }
}
