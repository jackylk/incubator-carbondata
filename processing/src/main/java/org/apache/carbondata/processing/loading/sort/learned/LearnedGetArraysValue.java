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
package org.apache.carbondata.processing.loading.sort.learned;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;

public class LearnedGetArraysValue implements GetArraysValue<Object[]> {

  private DataType[] noDicDataTypes;

  private boolean[] noDicSortColumnMapping;

  private SerializableLearnedDistribution learnDistribution;

  private boolean isHaveNull = false;

  public LearnedGetArraysValue(boolean[] noDicSortColumnMapping, DataType[] noDicDataTypes) {
    this.noDicSortColumnMapping = noDicSortColumnMapping;
    this.noDicDataTypes = noDicDataTypes;
  }

  public void initSortColumnsLength(Object[] row, int[] maxLength, int[] minLength,
      int maxStringLength) {
    int dataTypeIdx = 0;
    if ("STRING".equals(noDicDataTypes[dataTypeIdx].toString())) {
      maxLength[0] = 0;
      minLength[0] = maxStringLength;
      learnDistribution = LearnedDistribution.getDistribution(DataTypes.STRING);
    } else if (DataTypes.TIMESTAMP == noDicDataTypes[dataTypeIdx]
        || DataTypes.DATE == noDicDataTypes[dataTypeIdx]) {
      minLength[0] = maxLength[0] = 8;
      learnDistribution = LearnedDistribution.getDistribution(noDicDataTypes[dataTypeIdx]);
    } else {
      minLength[0] = maxLength[0] = noDicDataTypes[dataTypeIdx].getSizeInBytes();
      learnDistribution = LearnedDistribution.getDistribution(noDicDataTypes[dataTypeIdx]);
    }
  }

  public int getSortColumnsNumber(Object[] row) {
    return noDicSortColumnMapping.length;
  }

  public void distributionRange(Object[] row, int[] maxLength, int[] minLength, int[] learnFirst,
      int maxStringLength) {
    int index = 0;
    int dataTypeIdx = 0;
    int noDicSortIdx = 0;
    for (int i = 0; i < noDicSortColumnMapping.length; i++) {
      if (noDicSortColumnMapping[noDicSortIdx++]) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[dataTypeIdx])) {
          if (row[index] == null) {
            isHaveNull = true;
            return;
          }
          learnDistribution.getLearnDistribution(row[index], learnFirst);
          return;
        } else {
          byte[] byteArr = (byte[]) row[index];
          int length = byteArr.length;
          if (length > maxStringLength) {
            length = maxStringLength;
          }
          maxLength[0] = maxLength[0] <= length ? length : maxLength[0];
          minLength[0] = minLength[0] >= length ? length : minLength[0];
          int mid = 0;
          while (mid != length) {
            learnFirst[mid * 256 + (byteArr[mid] & 0xff)] = 1;
            mid++;
          }
          return;
        }
      }
    }

  }

  public int getLocateValue(Object[] row, int[] charWeight, int[] beginCharWeight, int beginNumber,
      int endNumber, int[] weightFinal, int[] minByte) {
    int index = 0;
    int dataTypeIdx = 0;
    int noDicSortIdx = 0;

    for (int i = 0; i < noDicSortColumnMapping.length; i++) {
      if (noDicSortColumnMapping[noDicSortIdx++]) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[dataTypeIdx])) {
          if (row[index] == null) {
            return 0;
          }
          return learnDistribution
              .getCondenseNumber(row[index], beginNumber, endNumber, charWeight, beginCharWeight,
                  minByte, weightFinal);
        } else {
          byte[] byteArr = (byte[]) row[index];
          int length = byteArr.length;
          int mid = beginNumber;
          int midResult = 0;
          if (length > endNumber) {
            while (mid != endNumber) {
              midResult = midResult
                  + charWeight[beginCharWeight[mid] + ((byteArr[mid] & 0xFF) - minByte[mid])]
                  * weightFinal[mid - beginNumber];
              mid++;
            }
            return midResult;
          } else {
            while (mid < length) {
              midResult = midResult
                  + charWeight[beginCharWeight[mid] + ((byteArr[mid] & 0xFF) - minByte[mid])]
                  * weightFinal[mid - beginNumber];
              mid++;
            }
            return midResult;
          }
        }
      }
    }
    return 0;
  }

  public boolean getIsHaveNull() {
    return isHaveNull;
  }

}
