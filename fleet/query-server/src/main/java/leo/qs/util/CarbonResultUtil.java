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

package leo.qs.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;

import leo.model.view.Column;
import leo.model.view.Schema;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.map.ObjectMapper;

public class CarbonResultUtil {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonResultUtil.class.getName());
  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static String convertSchemaToJson(StructType structType) throws Exception {
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
    String schemaJson = MAPPER.writeValueAsString(schema);
    return schemaJson;
  }

  public static Schema convertJsonToSchema(String schemaJson) throws Exception {
    Schema schema = MAPPER.readValue(schemaJson, Schema.class);
    return schema;
  }

}
