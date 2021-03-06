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

package org.apache.carbondata.core.datastore.page.statistics;

import org.apache.carbondata.core.util.ByteUtil;

/**
 * This class is for the columns with varchar data type,
 * a string type which can hold more than 32000 characters
 */
public class LVLongStringStatsCollector extends LVStringStatsCollector {

  public static LVLongStringStatsCollector newInstance() {
    return new LVLongStringStatsCollector();
  }

  private LVLongStringStatsCollector() {

  }

  @Override
  protected byte[] getActualValue(byte[] value) {
    byte[] actualValue;
    assert (value.length >= 4);
    if (value.length == 4) {
      assert (value[0] == 0 && value[1] == 0);
      actualValue = new byte[0];
    } else {
      int length = ByteUtil.toInt(value, 0);
      assert (length > 0);
      actualValue = new byte[value.length - 4];
      System.arraycopy(value, 4, actualValue, 0, actualValue.length);
    }
    return actualValue;
  }
}
