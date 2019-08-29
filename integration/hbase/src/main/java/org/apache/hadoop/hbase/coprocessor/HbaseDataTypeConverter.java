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

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.Field;

import com.huawei.cloudtable.leo.HBaseRowKeyCodec;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.HBaseTableReferencePart;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDataTypeConverter implements DataTypeConverter {

  private static SimpleDateFormat dateFormat =
      new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);

  private static SimpleDateFormat timeFormat =
      new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

  @Override
  public void convertRowKey(byte[] key, int offset, int len, int[] mapping, Field[] fields,
      String[] row) {
    if (mapping.length == 1) {
      row[mapping[0]] = convert(key, offset, len, fields[mapping[0]].getDataType());
    } else {
      for (int i = 0; i < mapping.length; i++) {
        DataType dataType = fields[mapping[i]].getDataType();
        int id = dataType.getId();
        if (id == DataTypes.BOOLEAN.getId()) {
          row[mapping[i]] = String.valueOf(key[offset] != (byte) 0);
          offset += 1;
        } else if (id == DataTypes.STRING.getId()) {
          int strLen = Bytes.toInt(key, offset, 4);
          offset += 4;
          row[mapping[i]] = Bytes.toString(key, offset, strLen);
          offset += strLen;
        } else if (id == DataTypes.INT.getId()) {
          row[mapping[i]] = String.valueOf(Bytes.toInt(key, offset, 4));
          offset += 4;
        } else if (id == DataTypes.SHORT.getId()) {
          row[mapping[i]] = String.valueOf(Bytes.toShort(key, offset, 2));
          offset += 2;
        } else if (id == DataTypes.LONG.getId()) {
          row[mapping[i]] = String.valueOf(Bytes.toLong(key, offset, 8));
          offset += 8;
        } else if (id == DataTypes.DOUBLE.getId()) {
          row[mapping[i]] = String.valueOf(Bytes.toDouble(key, offset));
          offset += 8;
        } else if (DataTypes.isDecimal(dataType)) {
          int decLen = key[offset];
          offset += 1;
          row[mapping[i]] = String.valueOf(Bytes.toBigDecimal(key, offset, decLen));
          offset += decLen;
        } else if (id == DataTypes.DATE.getId()) {
          row[mapping[i]] = dateFormat.format(new Date(Bytes.toInt(key, offset, 4) *
              org.apache.carbondata.core.keygenerator.directdictionary.timestamp
                  .DateDirectDictionaryGenerator.MILLIS_PER_DAY));
          offset += 4;
        } else if (id == DataTypes.TIMESTAMP.getId()) {
          row[mapping[i]] = timeFormat.format(new Date(Bytes.toLong(key, offset, 8)));
          offset += 8;
        } else if (id == DataTypes.VARCHAR.getId()) {
          int strLen = Bytes.toInt(key, offset, 4);
          offset += 4;
          row[mapping[i]] = Bytes.toString(key, offset, strLen);
          offset += strLen;
        } else if (id == DataTypes.FLOAT.getId()) {
          row[mapping[i]] = String.valueOf(Bytes.toFloat(key, offset));
          offset += 4;
        } else if (id == DataTypes.BYTE.getId()) {
          row[mapping[i]] = String.valueOf(key[offset]);
        } else {
          throw new UnsupportedOperationException(
              "Provided datatype " + dataType + " is not supported");
        }
      }
    }
  }

  @Override
  public String convert(byte[] value, int offset, int len, DataType dataType) {
    int id = dataType.getId();
    if (id == DataTypes.BOOLEAN.getId()) {
      return String.valueOf(value[offset] != (byte) 0);
    } else if (id == DataTypes.STRING.getId()) {
      return Bytes.toString(value, offset, len);
    } else if (id == DataTypes.INT.getId()) {
      return String.valueOf(Bytes.toInt(value, offset, len));
    } else if (id == DataTypes.SHORT.getId()) {
      return String.valueOf(Bytes.toShort(value, offset, len));
    } else if (id == DataTypes.LONG.getId()) {
      return String.valueOf(Bytes.toLong(value, offset, len));
    } else if (id == DataTypes.DOUBLE.getId()) {
      return String.valueOf(Bytes.toDouble(value, offset));
    } else if (DataTypes.isDecimal(dataType)) {
      return String.valueOf(Bytes.toBigDecimal(value, offset, len));
    } else if (id == DataTypes.DATE.getId()) {
      return dateFormat.format(new Date(Bytes.toInt(value, offset, len) *
          org.apache.carbondata.core.keygenerator.directdictionary.timestamp
              .DateDirectDictionaryGenerator.MILLIS_PER_DAY));
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return timeFormat.format(new Date(Bytes.toLong(value, offset, len)));
    } else if (id == DataTypes.VARCHAR.getId()) {
      return String.valueOf(Bytes.toString(value, offset, len));
    } else if (id == DataTypes.FLOAT.getId()) {
      return String.valueOf(Bytes.toFloat(value, offset));
    } else if (id == DataTypes.BYTE.getId()) {
      return String.valueOf(value[offset]);
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }

  @Override
  public void convertRowKeyWithDecode(HBaseTableReference tableReference, int primaryKeysNum,
      byte[] key, int offset, int len, int[] mapping, Field[] fields, String[] row) {
    HBaseRowKeyCodec.Decoder primaryKeyDecoder = tableReference.getPrimaryKeyCodec().decoder();
    primaryKeyDecoder.setRowKey(key, offset, len);
    for (int i = 0; i < mapping.length; i++) {
      primaryKeyDecoder.next();
      row[mapping[i]] = String.valueOf(primaryKeyDecoder.get());
    }
  }

  @Override
  public String convertWithDecode(byte[] value, int offset, int len, DataType dataType) {
    int id = dataType.getId();
    if (id == DataTypes.BOOLEAN.getId()) {
      Boolean valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Boolean.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.STRING.getId()) {
      String valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(String.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.INT.getId()) {
      int valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Integer.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.SHORT.getId()) {
      Short valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Short.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.LONG.getId()) {
      Long valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Long.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.DOUBLE.getId()) {
      Double valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Double.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (DataTypes.isDecimal(dataType)) {
      BigDecimal valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(BigDecimal.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.DATE.getId()) {
      Integer valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Integer.class)
          .decode(value, offset, len);
      return dateFormat.format(new Date(valueDec *
          org.apache.carbondata.core.keygenerator.directdictionary.timestamp
              .DateDirectDictionaryGenerator.MILLIS_PER_DAY));
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      Long valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Long.class)
          .decode(value, offset, len);
      return timeFormat.format(new Date(valueDec));
    } else if (id == DataTypes.VARCHAR.getId()) {
      return String.valueOf(Bytes.toString(value, offset, len));
    } else if (id == DataTypes.FLOAT.getId()) {
      Float valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Float.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else if (id == DataTypes.BYTE.getId()) {
      Byte valueDec = HBaseValueCodecManager.BUILD_IN.getValueCodec(Byte.class)
          .decode(value, offset, len);
      return String.valueOf(valueDec);
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }

}
