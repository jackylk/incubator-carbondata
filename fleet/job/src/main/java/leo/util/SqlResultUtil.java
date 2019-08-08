/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package leo.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import leo.model.view.Column;
import leo.model.view.Schema;
import leo.model.view.SqlResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class SqlResultUtil {

  /**
   * convert Dataset<Row> to SqlResponse
   *
   * @param rows
   * @return
   */
  public static SqlResult convertRows(Dataset<Row> rows) {
    Schema schema = convertSchema(rows.schema());

    List<String[]> strRows = new ArrayList<>();
    for (Row row : rows.collectAsList()) {
      String[] records = new String[row.size()];
      for (int i = 0; i < row.size(); i++) {
        records[i] = row.get(i) == null ? null : row.get(i).toString();
      }
      strRows.add(records);
    }

    return new SqlResult(schema, strRows);
  }


  /**
   * convert DataSet.schema to SqlResponse.schema
   *
   * @param structType
   * @return
   * @throws Exception
   */
  private static Schema convertSchema(StructType structType) {
    StructField[] fields = structType.fields();
    Schema schema = new Schema();
    List<Column> columns = new ArrayList<Column>();
    for (StructField field : fields) {
      Column column = new Column();
      column.setDataType(field.dataType().typeName());
      column.setName(field.name());
      column.setNullable(field.nullable());
      columns.add(column);
    }
    schema.setColumns(columns);

    return schema;
  }

  /**
   * build rows and schema from json object
   * only work for single level json object
   *
   * @param o
   * @return
   */
  public static SqlResult buildSqlResult(Object o) {
    Schema schema = new Schema();
    List<Column> columns = new ArrayList<>();

    // there are multi columns, but only one row.
    columns = getColumnsSchema(o);
    schema.setColumns(columns);

    List<String[]> rows = new ArrayList<>();

    List<String> fieldsName = getFieldsName(o);
    String[] row = new String[fieldsName.size()];
    for (int i = 0; i < fieldsName.size(); i++) {
      row[i] = getFieldValueByName(fieldsName.get(i), o);
    }
    rows.add(row);

    SqlResult response = new SqlResult();
    response.setSchema(schema);
    response.setRows(rows);

    return response;
  }

  private static String getFieldValueByName(String fieldName, Object o) {
    try {
      String firstLetter = fieldName.substring(0, 1).toUpperCase();
      String getter = "get" + firstLetter + fieldName.substring(1);
      Method method = o.getClass().getMethod(getter, new Class[]{});
      Object value = method.invoke(o, new Object[]{});
      return value.toString();
    } catch (Exception e) {

      return null;
    }
  }

  private static List<Column> getColumnsSchema(Object o) {
    List<Column> columns = new ArrayList<>();
    List<Class<?>> types = getFieldsType(o);
    List<String> fieldsName = getAnnotationFieldsName(o);
    // there are multi columns, but only one row.
    for (int i = 0; i < types.size(); i++) {
      // leaderEndpoint should not be displayed to user.
      Column column = new Column();
      String filedName = fieldsName.get(i);

      column.setName(fieldsName.get(i));
      Class<?> type = types.get(i);
      if (type == Boolean.class) {
        column.setDataType(ValueType.BOOLEAN.toString());
      } else if (type == Integer.class) {
        column.setDataType(ValueType.INT.toString());
      } else {
        column.setDataType(ValueType.VARCHAR.toString());
      }
      column.setNullable(false);
      columns.add(column);
    }
    return columns;
  }

  private static List<Class<?>> getFieldsType(Object o) {
    Field[] fields = o.getClass().getDeclaredFields();
    List<Class<?>> list = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      list.add(fields[i].getType());
    }
    return list;
  }


  private static List<String> getAnnotationFieldsName(Object o) {
    Field[] fields = o.getClass().getDeclaredFields();
    List<String> list = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      JsonProperty annotation = fields[i].getAnnotation(JsonProperty.class);
      if (annotation != null) {
        list.add(annotation.value());
      }
    }
    return list;
  }

  private static List<String> getFieldsName(Object o) {
    Field[] fields = o.getClass().getDeclaredFields();
    List<String> list = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      list.add(fields[i].getName());
    }

    return list;
  }

  public enum ValueType {
    BOOLEAN(0, "BOOLEAN"),
    INT(1, "INT"),
    VARCHAR(2, "VARCHAR");

    private Integer code;
    private String type;

    ValueType(Integer code, String type) {
      this.code = code;
      this.type = type;
    }
  }

}

