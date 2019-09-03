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

package org.apache.carbondata.processing.loading.sort.unsafe.sort;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUnsafeUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.sort.learned.LearnedDistribution;
import org.apache.carbondata.processing.loading.sort.learned.SerializableLearnedDistribution;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeCarbonRow;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

public class UnsafeGetSortValue extends GetSortValue<UnsafeCarbonRow> {

  private Object baseObject;

  private TableFieldStat tableFieldStat;

  private int dictSizeInMemory;

  private int sortColumnsNumber;

  private boolean isHaveNull = false;

  private SerializableLearnedDistribution learnDistribution;

  public UnsafeGetSortValue(UnsafeCarbonRowPage rowPage) {
    this.baseObject = rowPage.getDataBlock().getBaseObject();
    this.tableFieldStat = rowPage.getTableFieldStat();
    this.dictSizeInMemory = tableFieldStat.getDictSortDimCnt() * 4;
    this.sortColumnsNumber = tableFieldStat.getNoDictSortDimCnt();
  }

  /**
   * Return number of no dict sort
   */
  public int getSortColumnsNumber() {
    return this.sortColumnsNumber;
  }

  /**
   * Return the first sortColumn initial length
   */
  public void initSortColumnsLength(int[] maxLength, int[] minLength, int maxStringLength) {
    DataType dataType = tableFieldStat.getNoDictDataType()[0];
    if (DataTypes.STRING == dataType) {
      maxLength[0] = 0;
      minLength[0] = maxStringLength;
    } else if (DataTypes.TIMESTAMP == dataType || DataTypes.DATE == dataType) {
      minLength[0] = maxLength[0] = 8;
      learnDistribution = LearnedDistribution.getDistribution(dataType);
    } else {
      minLength[0] = maxLength[0] = dataType.getSizeInBytes();
      learnDistribution = LearnedDistribution.getDistribution(dataType);
    }
  }

  /**
   * Get the translate number
   *
   * @param row             the byte weight array
   * @param charWeight      Record the byte weight
   * @param beginCharWeight Record the starting position of the byte weight
   * @param beginNumber     The byte begin number
   * @param sortCharNumber  The number of thr first layer can distinguish
   * @param multiplyWeight  Record the starting position of the byte weight
   * @param minByte         the min value of the byte
   */
  public int getBucketValue(UnsafeCarbonRow row, int[] charWeight, int[] beginCharWeight,
                            int beginNumber, int sortCharNumber, int[] multiplyWeight, int[] minByte) {
    Object baseObject = this.baseObject;
    long rowAddr = row.address;
    int noDicSortIdx = 0;
    int sizeInNonDictPart = 0;
    for (boolean isNoDictionary : tableFieldStat.getIsSortColNoDictFlags()) {
      if (isNoDictionary) {
        short length = CarbonUnsafe.getUnsafe()
                .getShort(baseObject, rowAddr + dictSizeInMemory + sizeInNonDictPart);
        sizeInNonDictPart += 2;
        DataType dataType = tableFieldStat.getNoDictDataType()[noDicSortIdx]; // noDicSortIdx++
        if (DataTypeUtil.isPrimitiveColumn(dataType)) {
          if (0 != length) {
            Object data = CarbonUnsafeUtil
                    .getDataFromUnsafe(dataType, this.baseObject, row.address + dictSizeInMemory,
                            sizeInNonDictPart, length);
            return learnDistribution
                    .getCondenseNumber(data, beginNumber, sortCharNumber, charWeight, beginCharWeight,
                            minByte, multiplyWeight);
          }
          return 0;
        } else {
          byte[] byteArr = new byte[length];
          CarbonUnsafe.getUnsafe()
                  .copyMemory(baseObject, rowAddr + dictSizeInMemory + sizeInNonDictPart, byteArr,
                          CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
          int mid = beginNumber;
          int midResult = 0;
          if (sortCharNumber < length) {
            while (mid != sortCharNumber) {
              midResult = midResult
                      + charWeight[beginCharWeight[mid] + ((byteArr[mid] & 0xFF) - minByte[mid])]
                      * multiplyWeight[mid - beginNumber];
              mid++;
            }
            return midResult;
          } else {
            while (mid < length) {
              midResult = midResult
                      + charWeight[beginCharWeight[mid] + ((byteArr[mid] & 0xFF) - minByte[mid])]
                      * multiplyWeight[mid - beginNumber];
              mid++;
            }
            return midResult;
          }
        }
      }
    }
    return 0;
  }

  /**
   * Learn the byte distribution
   *
   * @param row        Original data
   * @param maxLength  The max length of byte array
   * @param minLength  The min length of byte array
   * @param learnFirst The array to save learned result
   */
  public void byteDistributionRange(UnsafeCarbonRow row, int[] maxLength, int[] minLength,
                                    int[] learnFirst, int maxStringLength) {
    Object baseObject = this.baseObject;
    long rowAddr = row.address;
    int noDicSortIdx = 0;
    int sizeInNonDictPart = 0;
    for (boolean isNoDictionary : tableFieldStat.getIsSortColNoDictFlags()) {
      if (isNoDictionary) {
        int length = CarbonUnsafe.getUnsafe()
                .getShort(baseObject, rowAddr + dictSizeInMemory + sizeInNonDictPart);
        sizeInNonDictPart += 2;
        DataType dataType = tableFieldStat.getNoDictDataType()[noDicSortIdx++];
        if (DataTypeUtil.isPrimitiveColumn(dataType)) {
          if (0 != length) {
            Object data = CarbonUnsafeUtil
                    .getDataFromUnsafe(dataType, this.baseObject, row.address + dictSizeInMemory,
                            sizeInNonDictPart, length);
            learnDistribution.getLearnDistribution(data, learnFirst);
            return;
          }
          isHaveNull = true;
        } else {
          if (length > maxStringLength) {
            length = maxStringLength;
          }
          maxLength[0] = maxLength[0] <= length ? length : maxLength[0];
          minLength[0] = minLength[0] >= length ? length : minLength[0];
          byte[] byteArr = new byte[length];
          CarbonUnsafe.getUnsafe()
                  .copyMemory(baseObject, rowAddr + dictSizeInMemory + sizeInNonDictPart, byteArr,
                          CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
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

  /**
   * Return whether has null data
   */
  public boolean getIsHaveNull() {
    return isHaveNull;
  }

}