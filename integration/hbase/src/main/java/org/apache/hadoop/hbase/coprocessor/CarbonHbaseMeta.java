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

package org.apache.hadoop.hbase.coprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.carbondata.sdk.file.Schema;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CarbonHbaseMeta {
  private static final Logger LOG = LoggerFactory.getLogger(CarbonReplicationEndpoint.class);

  private Schema schema;

  private Map<String, String> tblProperties;

  private Map<QualifierArray, Integer> schemaMapping;

  private int[] rowKeyMapping;

  private int timestampMap = -1;

  private int deleteStatusMap = -1;

  private QualifierArray temp = new QualifierArray();

  private String primaryKeyColumns;

  private DataTypeConverter dataTypeConverter;

  public CarbonHbaseMeta(Schema schema, Map<String, String> tblProperties) {
    this.schema = schema;
    this.tblProperties = tblProperties;
    dataTypeConverter = new HbaseDataTypeConverter();
    createSchemaMapping();
  }

  private void createSchemaMapping() {
    String keyName = "key";
    String timestamp = "timestamp";
    String status = "deletestatus";
    schemaMapping = new HashMap<>();
    List<Integer> keyMapping = new ArrayList<>();
    List<String> primaryKeyMapping = new ArrayList<>();
    String hbase_mapping = tblProperties.get("hbase_mapping");
    String[] split = hbase_mapping.split(",");
    for (String s : split) {
      String[] map = s.toLowerCase().split("=");
      if (map.length < 2) {
        throw new UnsupportedOperationException("Hbase mapping is not right " + s);
      }
      String[] qualifiers = map[0].split(":");
      if (map[0].equalsIgnoreCase(keyName)) {
        keyMapping.add(getSchemaIndex(map[1]));
        primaryKeyMapping.add(map[1]);
      } else if (map[0].equalsIgnoreCase(timestamp)) {
        timestampMap = getSchemaIndex(map[1]);
      } else if (map[0].equalsIgnoreCase(status)) {
        deleteStatusMap = getSchemaIndex(map[1]);
      } else {
        if (qualifiers.length < 2) {
          throw new UnsupportedOperationException(
              "Hbase mapping is not right, please make sure to provide column family and qualifier "
                  + map[0]);
        }
        int schemaIndex = getSchemaIndex(map[1]);
        byte[] cf = Bytes.toBytesBinary(qualifiers[0]);
        byte[] qual = Bytes.toBytesBinary(qualifiers[1]);
        schemaMapping.put(new QualifierArray(cf, 0, cf.length, qual, 0, qual.length), schemaIndex);
      }
    }
    rowKeyMapping = ArrayUtils.toPrimitive(keyMapping.toArray(new Integer[0]));
    if (timestampMap == -1) {
      throw new UnsupportedOperationException(
          "Time stamp mapping is mandatory for hbase, "
              + "please use timestamp in hbase_mapping carbon property inside schema");
    }
    if (deleteStatusMap == -1) {
      throw new UnsupportedOperationException(
          "Delete status mapping is mandatory for hbase, "
              + "please use deletestatus in hbase_mapping carbon property inside schema");
    }
    primaryKeyColumns = primaryKeyMapping.stream().collect(Collectors.joining(","));
  }

  private int getSchemaIndex(String columnName) {
    for (int i = 0; i < schema.getFields().length; i++) {
      if (schema.getFields()[i].getFieldName().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    throw new RuntimeException("Schema column mapping is not right " + columnName);
  }

  public int getSchemaIndexOfColumn(byte[] cf, int cfOffset, int cfLen, byte[] qual, int qualOffset,
      int qualLen) {
    temp.set(cf, cfOffset, cfLen, qual, qualOffset, qualLen);
    Integer integer = schemaMapping.get(temp);
    if (integer == null) {
      // hbase codec has column delimiter, which is a cell with "cf:", we just ignore it.
      LOG.debug("A delimiter column found: cf is: " + Bytes.toString(cf, cfOffset, cfLen)
       + " , qulifier is: " + Bytes.toString(qual, qualOffset, qualLen));
      return -1;
    } else {
      return integer;
    }
  }

  public Map<QualifierArray, Integer> getSchemaMapping() {
    return schemaMapping;
  }

  public int[] getKeyColumnIndex() {
    // TODO row key split needed to be supported.
    return rowKeyMapping;
  }

  public int getTimestampMapIndex() {
    return timestampMap;
  }

  public int getDeleteStatusMap() {
    return deleteStatusMap;
  }

  public Schema getSchema() {
    return schema;
  }

  public Map<String, String> getTblProperties() {
    return tblProperties;
  }

  public String getPrimaryKeyColumns() {
    return primaryKeyColumns;
  }

  public DataTypeConverter getDataTypeConverter() {
    return dataTypeConverter;
  }

}