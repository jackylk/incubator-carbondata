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

import java.nio.charset.StandardCharsets;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public class LearnedDistribution {

  private static final int SUPPORT_MAX_LENGTH = 50;

  public static SerializableLearnedDistribution getDistribution(DataType dataType) {
    if (dataType == DataTypes.INT) {
      return new IntSerializableLearnedDistribution();
    } else if (dataType == DataTypes.SHORT) {
      return new ShortSerializableLearnedDistribution();
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.DATE
        || dataType == DataTypes.TIMESTAMP) {
      return new LongSerializableLearnedDistribution();
    } else {
      return new StringSerializableLearnedDistribution();
    }
  }

  static class IntSerializableLearnedDistribution implements SerializableLearnedDistribution {
    @Override
    public void learnPartitionDistribution(Object key, short[] learnFirstResult, int[] minLength,
        int[] maxLength, int[] nullNumberData, int num) {
      if (key == null) {
        nullNumberData[num]++;
        return;
      }
      int key0 = (int) key;
      learnFirstResult[((key0 & 0xFFFF0000) >> 16) + 32768] = 1;
      learnFirstResult[((key0 & 0x0000FFFF)) + 65536] = 1;
    }

    @Override public void getLearnDistribution(Object key, int[] learnFirst) {
      learnFirst[(((int) key & 0xFF000000) >> 24) + 128] = 1;
      learnFirst[(((int) key & 0x00FF0000) >>> 16) + 256] = 1;
      learnFirst[(((int) key & 0x0000FF00) >>> 8) + 512] = 1;
      learnFirst[((int) key & 0x000000FF) + 768] = 1;
    }

    @Override
    public int getCondenseNumber(Object key, int beginNumber, int sortCharNumber, int[] charWeight,
        int[] beginCharWeight, int[] minByte, int[] weightFinal) {
      int data = (int) key;
      int midResult = 0;

      if (beginNumber == 0) {
        midResult = midResult
            + charWeight[beginCharWeight[0] + ((data & 0xFF000000) >> 24) + 128 - minByte[0]]
            * weightFinal[0];
      } else {
        midResult = midResult + charWeight[
            beginCharWeight[beginNumber] + ((data & (0xFF000000 >>> (beginNumber * 8))) >> (24
                - beginNumber * 8)) - minByte[beginNumber]] * weightFinal[0];
      }

      for (int i = beginNumber + 1; i < sortCharNumber; i++) {
        midResult = midResult +
            charWeight[beginCharWeight[i] + ((data & (0xFF000000 >>> (i * 8))) >> (24 - i * 8))
                - minByte[i]] * weightFinal[i - beginNumber];
      }
      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight) {
      int data = (int) key;
      int midResult;
      midResult = weightTable[((data & 0xFFFF0000) >> 16) + 32768 - minDoubleByte[0]]
          * multiplyWeight[byteMultiWeight];
      midResult += weightTable[bytePositionWeightTable[1] + (data & 0x0000FFFF) - minDoubleByte[1]]
          * multiplyWeight[byteMultiWeight + 1];
      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight,
        int sortDoubleByteForPartion) {
      int data = (int) key;
      int midResult;
      midResult = weightTable[((data & 0xFFFF0000) >> 16) + 32768 - minDoubleByte[0]]
          * multiplyWeight[byteMultiWeight];
      if (sortDoubleByteForPartion == 2) {
        midResult +=
            weightTable[bytePositionWeightTable[1] + (data & 0x0000FFFF) - minDoubleByte[1]]
                * multiplyWeight[byteMultiWeight + 1];
      }
      return midResult;
    }

    @Override public String antiForStr(int[] midResult, int[] midMinDoubleByte) {
      int res = 0;
      for (int j = 0; j < midResult.length; j++) {
        if (j == 0) {
          res = res + (midResult[j] + midMinDoubleByte[j] - 32768) * 65536;
        } else {
          res = res + (midResult[j] + midMinDoubleByte[j]);
        }
      }
      return res + ",";
    }
  }

  static class ShortSerializableLearnedDistribution implements SerializableLearnedDistribution {
    @Override
    public void learnPartitionDistribution(Object key, short[] learnFirstResult, int[] minLength,
        int[] maxLength, int[] nullNumberData, int num) {
      if (key == null) {
        nullNumberData[num]++;
        return;
      }
      short key0 = (short) key;
      learnFirstResult[key0 + 32768] = 1;
    }

    @Override public void getLearnDistribution(Object key, int[] learnFirst) {
      int mid = (((short) key & 0xFF00) >> 8);
      if (mid > 127) {
        learnFirst[(((short) key & 0xFF00) >> 8) - 128] = 1;
      } else {
        learnFirst[(((short) key & 0xFF00) >> 8) + 128] = 1;
      }
      learnFirst[(((short) key & 0x00FF)) + 256] = 1;
    }

    @Override
    public int getCondenseNumber(Object key, int beginNumber, int sortCharNumber, int[] charWeight,
        int[] beginCharWeight, int[] minByte, int[] weightFinal) {
      short data = (short) key;
      int midResult = 0;
      if (beginNumber == 0) {
        int mid = ((data & 0xFF00) >> 8);
        if (mid > 127) {
          mid = mid - 128;
        } else {
          mid = mid + 128;
        }
        midResult = midResult
            + charWeight[beginCharWeight[0] + mid - minByte[beginNumber]] * weightFinal[0];
      } else {
        midResult = midResult
            + charWeight[beginCharWeight[beginNumber] + (data & (0x00FF) - minByte[beginNumber])]
            * weightFinal[0];
      }

      for (int i = beginNumber + 1; i < sortCharNumber; i++) {
        midResult = midResult
            + charWeight[beginCharWeight[i] + (data & (0x00FF)) - minByte[i]] * weightFinal[i
            - beginNumber];
      }
      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight) {
      short data = (short) key;
      return weightTable[data + 32768 - minDoubleByte[0]] * multiplyWeight[byteMultiWeight];
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight,
        int sortDoubleByteForPartion) {
      short data = (short) key;
      return weightTable[data + 32768 - minDoubleByte[0]] * multiplyWeight[byteMultiWeight];
    }

    @Override public String antiForStr(int[] midResult, int[] midMinDoubleByte) {
      int res = midResult[0] + midMinDoubleByte[0] - 32768;
      return res + ",";
    }

  }

  static class LongSerializableLearnedDistribution implements SerializableLearnedDistribution {
    @Override
    public void learnPartitionDistribution(Object key, short[] learnFirstResult, int[] minLength,
        int[] maxLength, int[] nullNumberData, int num) {
      if (key == null) {
        nullNumberData[num]++;
        return;
      }
      long key0 = (long) key;
      learnFirstResult[(int) ((key0 & 0xFFFF000000000000L) >> 48) + 32768] = 1;
      learnFirstResult[65536 + (int) ((key0 & 0x0000FFFF00000000L) >> 32)] = 1;
      learnFirstResult[65536 * 2 + (int) ((key0 & 0x00000000FFFF0000L) >> 16)] = 1;
      learnFirstResult[65536 * 3 + (int) (key0 & 0x000000000000FFFFL)] = 1;
    }

    @Override public void getLearnDistribution(Object key, int[] learnFirst) {
      long data = (long) key;
      learnFirst[(int) ((data & 0xFF00000000000000L) >> 56) + 128] = 1;
      for (int i = 1; i < 8; i++) {
        learnFirst[256 * i + (int) (((long) key & (0xFF00000000000000L >>> (i * 8))) >> (56
            - i * 8))] = 1;
      }
    }

    @Override
    public int getCondenseNumber(Object key, int beginNumber, int sortCharNumber, int[] charWeight,
        int[] beginCharWeight, int[] minByte, int[] weightFinal) {
      long data = (long) key;
      int midResult = 0;

      if (beginNumber == 0) {
        int midData = (int) ((data & 0xFF00000000000000L) >> 56);
        midResult = midResult
            + charWeight[beginCharWeight[0] + midData + 128 - minByte[0]] * weightFinal[0];
      } else {
        midResult = midResult + charWeight[beginCharWeight[beginNumber] + (int) (
            (data & (0xFF00000000000000L >>> (beginNumber * 8))) >> (56 - beginNumber * 8))
            - minByte[beginNumber]] * weightFinal[0];
      }
      for (int i = beginNumber + 1; i < sortCharNumber; i++) {
        midResult = midResult + charWeight[
            beginCharWeight[i] + (int) ((data & (0xFF00000000000000L >>> (i * 8))) >> (56 - i * 8))
                - minByte[i]] * weightFinal[i - beginNumber];
      }
      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight) {
      long data = (long) key;
      int midResult = 0;
      midResult += weightTable[
          bytePositionWeightTable[0] + (int) ((data & 0xFFFF000000000000L) >> 48) + 32768
              - minDoubleByte[0]] * multiplyWeight[byteMultiWeight];
      midResult +=
          weightTable[bytePositionWeightTable[1] + (int) ((data & 0x0000FFFF00000000L) >> 32)
              - minDoubleByte[1]] * multiplyWeight[byteMultiWeight + 1];
      midResult +=
          weightTable[bytePositionWeightTable[2] + (int) ((data & 0x00000000FFFF0000L) >> 16)
              - minDoubleByte[2]] * multiplyWeight[byteMultiWeight + 2];
      midResult += weightTable[bytePositionWeightTable[3] + (int) (data & 0x000000000000FFFFL)
          - minDoubleByte[3]] * multiplyWeight[byteMultiWeight + 3];

      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight,
        int sortDoubleByteForPartion) {
      long data = (long) key;
      int midResult = 0;
      midResult += weightTable[
          bytePositionWeightTable[0] + (int) ((data & 0xFFFF000000000000L) >> 48) + 32768
              - minDoubleByte[0]] * multiplyWeight[byteMultiWeight];

      for (int i = 1; i < sortDoubleByteForPartion; i++) {
        midResult += weightTable[
            bytePositionWeightTable[i] + (int) ((data & (0xFFFF000000000000L >>> i * 16)) >> (16 * (
                3 - i))) - minDoubleByte[i]] * multiplyWeight[byteMultiWeight + i];
      }
      return midResult;
    }

    @Override public String antiForStr(int[] midResult, int[] midMinDoubleByte) {
      long res = 0;
      for (int j = 0; j < midResult.length; j++) {
        if (j == 0) {
          res = res + (midResult[j] + midMinDoubleByte[j] - 32768) * (long) Math.pow(65536, 3 - j);
        } else {
          res = res + (midResult[j] + midMinDoubleByte[j]) * (long) Math.pow(65536, 3 - j);
        }
      }
      return res + ",";
    }

  }

  static class StringSerializableLearnedDistribution implements SerializableLearnedDistribution {
    @Override
    public void learnPartitionDistribution(Object key, short[] learnFirstResult, int[] minLength,
        int[] maxLength, int[] nullNumberData, int num) {
      byte[] key0 = (byte[]) (key);
      int length = key0.length;
      if (length == 0) {
        nullNumberData[num]++;
        return;
      }
      minLength[num] = minLength[num] <= length ? minLength[num] : length;
      maxLength[num] = maxLength[num] >= length ? maxLength[num] : length;
      int doubleLength = length / 2;
      if (length > SUPPORT_MAX_LENGTH) {
        return;
      }
      for (int i = 0; i < doubleLength * 2; i = i + 2) {
        learnFirstResult[65536 * (i >> 1) + (key0[i] & 0xFF) * 256 + (key0[i + 1] & 0xFF)] = 1;
      }
      if ((length % 2) != 0) {
        learnFirstResult[doubleLength * 65536 + (key0[length - 1] & 0xFF) * 256] = 1;
      }
    }

    @Override public void getLearnDistribution(Object key, int[] learnFirst) {
      throw new UnsupportedOperationException();      //try catch
    }

    @Override
    public int getCondenseNumber(Object key, int beginNumber, int endNumber, int[] charWeight,
        int[] beginCharWeight, int[] minByte, int[] weightFinal) {
      throw new UnsupportedOperationException();
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight) {
      int midResult = 0;
      byte[] key0 = (byte[]) (key);
      int length = key0.length;
      int doubleLength = length / 2;
      int k = 0;
      for (int i = 0; i < doubleLength * 2; i = i + 2) {
        midResult +=
            weightTable[bytePositionWeightTable[k] + (key0[i] & 0xff) * 256 + (key0[i + 1] & 0xff)
                - minDoubleByte[k]] * multiplyWeight[byteMultiWeight + k++];
      }
      if ((length % 2) != 0) {
        midResult += weightTable[bytePositionWeightTable[k] + (key0[length - 1] & 0xff) * 256
            - minDoubleByte[k]] * multiplyWeight[byteMultiWeight + k];
      }

      return midResult;
    }

    @Override public int getSortPartition(Object key, int[] minDoubleByte, int[] weightTable,
        int[] bytePositionWeightTable, int[] multiplyWeight, int byteMultiWeight,
        int sortDoubleByteForPartion) {
      int midResult = 0;
      byte[] key0 = (byte[]) (key);
      int length = key0.length;
      length = length < sortDoubleByteForPartion * 2 ? length : sortDoubleByteForPartion * 2;
      int doubleLength = length / 2;
      int k = 0;
      for (int i = 0; i < doubleLength * 2; i = i + 2) {
        midResult +=
            weightTable[bytePositionWeightTable[k] + (key0[i] & 0xff) * 256 + (key0[i + 1] & 0xff)
                - minDoubleByte[k]] * multiplyWeight[byteMultiWeight + k];
        k++;
      }
      if ((length % 2) != 0) {
        midResult += weightTable[bytePositionWeightTable[k] + (key0[length - 1] & 0xff) * 256
            - minDoubleByte[k]] * multiplyWeight[byteMultiWeight + k];
      }
      return midResult;
    }

    @Override public String antiForStr(int[] midResult, int[] midMinDoubleByte) {
      StringBuilder result = new StringBuilder();
      for (int j = 0; j < midResult.length; j++) {
        midResult[j] = midResult[j] + midMinDoubleByte[j];
        byte[] midByte = new byte[2];
        midByte[0] = (byte) (midResult[j] / 256);
        midByte[1] = (byte) (midResult[j] % 256);
        String m = new String(midByte, StandardCharsets.UTF_8);
        result.append(m);
      }
      result.append(",");
      return result.toString();
    }

  }

}
