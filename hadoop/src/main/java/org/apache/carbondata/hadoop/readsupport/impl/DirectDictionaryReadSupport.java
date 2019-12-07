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

package org.apache.carbondata.hadoop.readsupport.impl;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

/**
 * A read support implementation to return Object array
 * Only does direct dictionary (date/timestamp) conversion
 */
public class DirectDictionaryReadSupport<T> implements CarbonReadSupport<T> {

  protected DataType[] dataTypes;

  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
      throws IOException {
    this.dataTypes = new DataType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      dataTypes[i] = carbonColumns[i].getDataType();
    }
  }

  @Override
  public T readRow(Object[] data) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (dataTypes[i] == DataTypes.DATE) {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(0));
        c.add(Calendar.DAY_OF_YEAR, (Integer) data[i]);
        c.add(Calendar.DATE, 1);
        data[i] = new Date(c.getTime().getTime());
      } else if (dataTypes[i] == DataTypes.TIMESTAMP) {
        data[i] = new Timestamp((long) data[i] / 1000);
      }
    }
    return (T)data;
  }

  @Override
  public void close() {
  }
}
