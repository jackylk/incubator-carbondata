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

import java.util.Comparator;

public class LearnedSort<K, Buffer> {

  private static final short SORT_THRESHOLD = 10000;

  private static final int SORT_LIMIT = 65536;

  private static final int FIRST_BUCKET = 256;

  private static final int MAX_STRING_LENGTH = 200;

  private static final int CHAR_VALUE = 256;

  private int low;

  private SortDataFormat<K, Buffer> s;

  private TimSort<K, Buffer> timSort;

  public LearnedSort(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
    this.timSort = new TimSort<>(sortDataFormat);
  }

  /**
   * Sorts the given range, using the given workspace array slice
   * for temp storage when possible. This method is designed to be
   * invoked from public methods (TimSort) after performing
   * any necessary array bounds checks and expanding parameters into
   * the required forms.
   *
   * @param a  the array to be sorted
   * @param lo the index of the first element, inclusive, to be sorted
   * @param hi the index of the last element, exclusive, to be sorted
   * @param g  the method of get value
   * @param c  the comparator to use
   * @since 1.8
   */
  public void sort(Buffer a, int lo, int hi, GetSortValue<K> g, Comparator<? super K> c) {
    assert c != null;
    int nRemaining = hi - lo;
    if (nRemaining < SORT_LIMIT) {
      timSort.sort(a, lo, hi, c); //If arrays is small, use timsort
      return;
    }

    low = lo;
    int sortColumnsNumber = g.getSortColumnsNumber();
    int[] maxLength = new int[1];
    int[] minLength = new int[1];
    g.initSortColumnsLength(maxLength, minLength, MAX_STRING_LENGTH);

    int[] initBucketSize = new int[1];   //The first layer bucket size
    initBucketSize[0] = FIRST_BUCKET;
    //Save the learned result
    int[] learnFirstResult = learnFirst(a, lo, hi, g, maxLength, minLength);

    int[] maxByte = new int[maxLength[0]];
    int[] minByte = new int[maxLength[0]];
    calculateMinMaxByte(learnFirstResult, maxLength, minLength, maxByte, minByte);
    int[] weight = getWeight(learnFirstResult, maxLength, minLength, maxByte, minByte);
    int[] beginPosition = getBeginPosition(maxByte, minByte);

    //If there is only one value,return directly or use Arrays.sort
    if (maxLength[0] == weight.length) {
      if (1 == sortColumnsNumber && !g.getIsHaveNull()) {
        if (maxLength[0] == MAX_STRING_LENGTH) {
          timSort.sort(a, lo, hi, c);
        }
        return;
      } else {
        timSort.sort(a, lo, hi, c);
        return;
      }
    }

    int[] sortCharNumber = new int[1];
    int[] multiplyWeight = getMultiplyWeight(weight, beginPosition, sortCharNumber, initBucketSize);

    int[] posBiasSort = new int[initBucketSize[0]];
    int truncationNumber =
            firstLayerSort(a, lo, hi, g, weight, beginPosition, sortCharNumber, minByte, multiplyWeight,
                    posBiasSort);

    if (1 == sortColumnsNumber && sortCharNumber[0] == maxLength[0]) {
      if (MAX_STRING_LENGTH == maxLength[0]) {
        bucketTimSort(a, c, hi, truncationNumber, posBiasSort);
        return;
      }
      if (g.getIsHaveNull()) {
        if (posBiasSort.length > 1) {
          timSort.sort(a, posBiasSort[0], posBiasSort[1], c);
        } else {
          timSort.sort(a, posBiasSort[0], hi, c);
        }
      }
      return;
    } else if (sortCharNumber[0] == maxLength[0]) {
      bucketTimSort(a, c, hi, truncationNumber, posBiasSort);
      return;
    }

    secondLayerSort(a, hi, g, c, weight, beginPosition, sortCharNumber, minByte, posBiasSort,
            truncationNumber, sortColumnsNumber, maxLength[0]);
  }

  /**
   * The first step to learn all data, get the byte distribution
   *
   * @param a         the array in which a range is to be sorted
   * @param lo        the index of the first element in the range to be sorted
   * @param hi        the index after the last element in the range to be sorted
   * @param g         tht method to get sort value
   * @param maxLength the sort value max byte
   * @param minLength the sort value min byte
   */
  private int[] learnFirst(Buffer a, int lo, int hi, GetSortValue<K> g, int[] maxLength,
                           int[] minLength) {
    int[] learn = new int[minLength[0] * CHAR_VALUE];
    K key = s.newKey();
    while (lo < hi) {
      g.byteDistributionRange(s.getKey(a, lo, key), maxLength, minLength, learn, MAX_STRING_LENGTH);
      lo++;
    }
    return learn;
  }

  /**
   * Based on the first learn result, get the min and max Byte
   *
   * @param learnFirstResult the byte distribution
   * @param maxLength        the sort value max byte
   * @param minLength        the sort value min byte
   * @param maxByte          the sort value max byte
   * @param minByte          the sort value max byte
   */
  private void calculateMinMaxByte(int[] learnFirstResult, int[] maxLength, int[] minLength,
                                   int[] maxByte, int[] minByte) {
    for (int i = 0; i < maxLength[0]; i++) {
      boolean flag = true;
      for (int j = 0; j < CHAR_VALUE; j++) {
        if (0 != learnFirstResult[i * CHAR_VALUE + j]) {
          if (flag) {
            minByte[i] = j;
            flag = false;
          }
          maxByte[i] = j;
        }
      }
    }
    //Reserve space for null characters
    for (int i = minLength[0]; i < maxLength[0]; i++) {
      minByte[i] = minByte[i] - 1;
    }
  }

  private int[] getWeight(int[] learnFirstResult, int[] maxLength, int[] minLength, int[] maxByte,
                          int[] minByte) {
    int weightRange = 0;
    for (int i = 0; i < maxLength[0]; i++) {
      weightRange = weightRange + maxByte[i] - minByte[i] + 1;
    }

    int[] subWeight = new int[weightRange];
    int inI = 0;
    for (int i = 0; i < maxLength[0]; i++) {
      int rem = 0;
      if (i >= minLength[0]) {
        rem = 1;
        subWeight[inI] = 0;
        inI++;
      }
      for (int j = minByte[i] + rem; j <= maxByte[i]; j++) {
        if (0 != learnFirstResult[i * CHAR_VALUE + j]) {
          subWeight[inI] = rem;
          rem++;
          inI++;
        } else {
          inI++;
        }
      }
    }
    return subWeight;
  }

  /**
   * Returns the char weight starting position
   *
   * @param maxByte the sort value max byte
   * @param minByte the sort value max byte
   */
  private int[] getBeginPosition(int[] maxByte, int[] minByte) {
    int[] beginPosition = new int[maxByte.length];
    for (int i = 1; i < beginPosition.length; i++) {
      beginPosition[i] = beginPosition[i - 1] + maxByte[i - 1] - minByte[i - 1] + 1;
    }
    return beginPosition;
  }

  /**
   * Returns the MultiplyWeight
   *
   * @param weight         the byte weight array
   * @param beginPosition  Record the starting position of the byte weight
   * @param sortCharNumber the number of thr first layer can distinguish
   * @param bucketNumber   the number of the bucket
   */
  private int[] getMultiplyWeight(int[] weight, int[] beginPosition, int[] sortCharNumber,
                                  int[] bucketNumber) {

    calcBucketAndCharNumber(beginPosition, weight, bucketNumber, sortCharNumber);

    if (sortCharNumber[0] < 2 && beginPosition.length > 1) {
      bucketNumber[0] = CHAR_VALUE * CHAR_VALUE;
      calcBucketAndCharNumber(beginPosition, weight, bucketNumber, sortCharNumber);
    }

    int[] weightMultiply = new int[sortCharNumber[0]];
    if (weightMultiply.length == 0) {
      return weightMultiply;
    }
    weightMultiply[weightMultiply.length - 1] = 1;
    for (int i = weightMultiply.length - 1; i > 0; i--) {
      if (i == (beginPosition.length - 1)) {
        weightMultiply[i - 1] = weightMultiply[i] * (weight[weight.length - 1] + 1);
        continue;
      }
      weightMultiply[i - 1] = weightMultiply[i] * (weight[beginPosition[i + 1] - 1] + 1);
    }

    return weightMultiply;
  }

  private void calcBucketAndCharNumber(int[] beginPosition, int[] weight, int[] bucketNumber,
                                       int[] sortCharNumber) {
    int midWeight = 1;
    for (int i = 0; i < beginPosition.length; i++) {
      if (i == (beginPosition.length - 1)) {
        midWeight = midWeight * (weight[weight.length - 1] + 1);
        if (midWeight > bucketNumber[0]) {
          midWeight = midWeight / (weight[weight.length - 1] + 1);
          sortCharNumber[0] = i;
          bucketNumber[0] = midWeight;
          break;
        }
        sortCharNumber[0] = i + 1;
        bucketNumber[0] = midWeight;
        break;
      }
      midWeight = midWeight * (weight[beginPosition[i + 1] - 1] + 1);
      if (midWeight > bucketNumber[0]) {
        midWeight = midWeight / ((weight[beginPosition[i + 1] - 1] + 1));
        sortCharNumber[0] = i;
        bucketNumber[0] = midWeight;
        break;
      }
    }
  }

  /**
   * The first layer to sort
   *
   * @param a              the byte weight array
   * @param lo             Record the starting position of the byte weight
   * @param hi             the number of thr first layer can distinguish
   * @param g              the number of the bucket
   * @param weight         the byte weight array
   * @param beginPosition  Record the starting position of the byte weight
   * @param sortCharNumber the number of thr first layer can distinguish
   * @param minByte        the min value of the byte
   * @param multiplyWeight returns the MultiplyWeight
   * @param posBiasSort    the sorted position
   */
  private int firstLayerSort(Buffer a, int lo, int hi, GetSortValue<K> g, int[] weight,
                             int[] beginPosition, int[] sortCharNumber, int[] minByte, int[] multiplyWeight,
                             int[] posBiasSort) {
    int[] posNumber = new int[posBiasSort.length];
    int midLo = lo;
    int midTarget;

    K key = s.newKey();
    while (midLo < hi) {
      midTarget =
              g.getBucketValue(s.getKey(a, midLo, key), weight, beginPosition, 0, sortCharNumber[0],
                      multiplyWeight, minByte);
      posNumber[midTarget]++;
      midLo++;
    }

    posBiasSort[0] = lo;
    calculateBias(posNumber, posBiasSort);

    transferData(a, lo, g, weight, beginPosition, 0, sortCharNumber[0], minByte, multiplyWeight,
            posBiasSort, posNumber);

    posBiasSort[0] = lo;
    return calculateBiasForSort(posNumber, posBiasSort);
  }

  /**
   * The first layer to sort
   *
   * @param a              the byte weight array
   * @param lo             Record the starting position of the byte weight
   * @param g              the number of the bucket
   * @param weight         the byte weight array
   * @param beginPosition  Record the starting position of the byte weight
   * @param sortCharNumber the number of thr first layer can distinguish
   * @param minByte        the min value of the byte
   * @param multiplyWeight returns the MultiplyWeight
   * @param posBiasSort    the sorted position
   * @param posNumber      the number of the bucket
   */
  private void transferData(Buffer a, int lo, GetSortValue<K> g, int[] weight, int[] beginPosition,
                            int beginNumber, int sortCharNumber, int[] minByte, int[] multiplyWeight, int[] posBiasSort,
                            int[] posNumber) {
    K key = s.newKey();
    int midNextTarget;
    int i = 0;
    int j;
    int posBiasLength = posBiasSort.length - 1;
    int sumPosNumber = lo;
    int swapAddress;
    int midTarget;
    while (i < posBiasLength) {
      sumPosNumber = sumPosNumber + posNumber[i];
      j = posBiasSort[i];
      while (j < sumPosNumber) {
        midTarget = g.getBucketValue(s.getKey(a, j, key), weight, beginPosition, beginNumber,
                sortCharNumber, multiplyWeight, minByte);
        if (midTarget == i) {
          j++;
          continue;
        }
        swapAddress = s.getPointer(a, j);
        midNextTarget =
                g.getBucketValue(s.getKey(a, posBiasSort[midTarget], key), weight, beginPosition,
                        beginNumber, sortCharNumber, multiplyWeight, minByte);
        if (midNextTarget == i) {
          s.swap(a, j, posBiasSort[midTarget]++);
          j++;
          continue;
        }

        while (midNextTarget != i) {
          while (midNextTarget == midTarget) {
            posBiasSort[midTarget]++;
            midNextTarget =
                    g.getBucketValue(s.getKey(a, posBiasSort[midTarget], key), weight, beginPosition,
                            beginNumber, sortCharNumber, multiplyWeight, minByte);
          }

          swapAddress = s.getAndSetAddress(a, posBiasSort[midTarget], swapAddress);
          posBiasSort[midTarget]++;
          midTarget = midNextTarget;

          if (midTarget == i) {
            break;
          }

          midNextTarget =
                  g.getBucketValue(s.getKey(a, posBiasSort[midTarget], key), weight, beginPosition,
                          beginNumber, sortCharNumber, multiplyWeight, minByte);

          if (midNextTarget == i) {
            swapAddress = s.getAndSetAddress(a, posBiasSort[midTarget]++, swapAddress);
          }
        }
        s.setAddress(a, j, swapAddress);
        j++;
      }
      i++;
    }
  }

  /**
   * @param posNumber   the number of the bucket
   * @param posBiasSort the position of the bucket
   */
  private static void calculateBias(int[] posNumber, int[] posBiasSort) {
    int sumNum = 0;
    int sumBiasSort = 1;
    int number = posNumber.length;
    while (sumBiasSort < number) {
      posBiasSort[sumBiasSort] = posBiasSort[sumBiasSort - 1] + posNumber[sumNum];
      sumBiasSort++;
      sumNum++;
    }
  }

  /**
   * returns the truncation of the sort
   *
   * @param posNumber   the number of the bucket
   * @param posBiasSort the sorted position
   */
  private static int calculateBiasForSort(int[] posNumber, int[] posBiasSort) {
    int sumKey = 1;
    int sumNum = 0;
    int sumBiasSort = 1;
    int number = posNumber.length;
    while (sumBiasSort < number) {
      if (posNumber[sumNum] != 0) {
        posBiasSort[sumKey] = posBiasSort[sumKey - 1] + posNumber[sumNum];
        sumKey++;
      }
      sumBiasSort++;
      sumNum++;
    }
    return sumKey - 1;
  }

  private void secondLayerSort(Buffer a, int hi, GetSortValue<K> g, Comparator<? super K> c,
                               int[] weight, int[] beginPosition, int[] sortCharNumber, int[] minByte, int[] posBiasSort,
                               int truncationNumber, int sortColumnsNumber, int maxLength) {
    for (int m = 0; m < truncationNumber; m++) {
      int timLo = posBiasSort[m];
      int timHi = posBiasSort[m + 1];
      if ((timHi - timLo) > SORT_THRESHOLD) {
        startSecondLayerSort(a, timLo, timHi, g, c, weight, beginPosition, sortCharNumber, minByte,
                sortColumnsNumber, maxLength);
      } else {
        timSort.sort(a, timLo, timHi, c);
      }
    }

    if ((hi - posBiasSort[truncationNumber]) > SORT_THRESHOLD) {
      startSecondLayerSort(a, posBiasSort[truncationNumber], hi, g, c, weight, beginPosition,
              sortCharNumber, minByte, sortColumnsNumber, maxLength);
    } else {
      timSort.sort(a, posBiasSort[truncationNumber], hi, c);
    }
  }

  private void startSecondLayerSort(Buffer a, int lo, int hi, GetSortValue<K> g,
                                    Comparator<? super K> c, int[] weight, int[] beginPosition, int[] sortCharNumber,
                                    int[] minByte, int sortColumnsNumber, int maxLength) {
    int[] sortSecondNumber = new int[1];
    int[] secondBucket = new int[1];
    secondBucket[0] = hi - lo;

    if (secondBucket[0] < 65536) {
      secondBucket[0] = 65536;
    }

    secondSortNumber(weight, beginPosition, sortCharNumber, sortSecondNumber, secondBucket);

    int endNumber = sortCharNumber[0] + sortSecondNumber[0];
    int[] weightFinal = new int[sortSecondNumber[0]];
    secondSortWeight(weight, beginPosition, sortCharNumber, sortSecondNumber, weightFinal);

    int[] posBiasSort = new int[secondBucket[0]];
    int[] posNumber = new int[secondBucket[0]];
    int midLo = lo;
    int midTarget;
    K key = s.newKey();
    while (midLo < hi) {
      midTarget =
              g.getBucketValue(s.getKey(a, midLo, key), weight, beginPosition, sortCharNumber[0],
                      endNumber, weightFinal, minByte);
      posNumber[midTarget]++;
      midLo++;
    }

    posBiasSort[0] = lo;
    calculateBias(posNumber, posBiasSort);

    // transfer data second
    transferData(a, lo, g, weight, beginPosition, sortCharNumber[0], endNumber, minByte,
            weightFinal, posBiasSort, posNumber);

    posBiasSort[0] = lo;
    int sortLine = calculateBiasForSort(posNumber, posBiasSort);

    if ((sortCharNumber[0] + sortSecondNumber[0]) == maxLength && 1 == sortColumnsNumber) {
      if (MAX_STRING_LENGTH == maxLength) {
        bucketTimSort(a, c, hi, sortLine, posBiasSort);
        return;
      }
      if (g.getIsHaveNull() && low == posBiasSort[0]) {
        if (sortLine == 0) {
          timSort.sort(a, posBiasSort[0], hi, c);
        } else {
          timSort.sort(a, posBiasSort[0], posBiasSort[1], c);
        }
      }
      return;
    }
    bucketTimSort(a, c, hi, sortLine, posBiasSort);
  }

  private void bucketTimSort(Buffer a, Comparator<? super K> c, int hi, int sortLine,
                             int[] posBiasSort) {
    for (int m = 0; m < sortLine; m++) {
      int timLo = posBiasSort[m];
      int timHi = posBiasSort[m + 1];
      timSort.sort(a, timLo, timHi, c);
    }
    timSort.sort(a, posBiasSort[sortLine], hi, c);
  }

  private void secondSortWeight(int[] charWeight, int[] beginCharWeight, int[] sortCharNumber,
                                int[] sortSecondNumber, int[] weightFinal) {
    weightFinal[weightFinal.length - 1] = 1;
    int midWeightSecond = weightFinal.length - 1;
    for (int i = sortCharNumber[0] + sortSecondNumber[0] - 1; i > sortCharNumber[0]; i--) {
      if (i == (beginCharWeight.length - 1)) {
        weightFinal[midWeightSecond - 1] =
                weightFinal[midWeightSecond] * (charWeight[charWeight.length - 1] + 1);
        midWeightSecond--;
        continue;
      }
      weightFinal[midWeightSecond - 1] =
              weightFinal[midWeightSecond] * (charWeight[beginCharWeight[i + 1] - 1] + 1);
      midWeightSecond--;
    }
  }

  private void secondSortNumber(int[] charWeight, int[] beginCharWeight, int[] sortCharNumber,
                                int[] sortSecondNumber, int[] secondBucket) {
    int midWeight = 1;
    for (int i = sortCharNumber[0]; i < beginCharWeight.length; i++) {
      if (i == (beginCharWeight.length - 1)) {
        midWeight = midWeight * (charWeight[charWeight.length - 1] + 1);
        if (midWeight > secondBucket[0]) {
          midWeight = midWeight / (charWeight[charWeight.length - 1] + 1);
          sortSecondNumber[0] = i - sortCharNumber[0];
          break;
        }
        sortSecondNumber[0] = i - sortCharNumber[0] + 1;
        break;
      }
      midWeight = midWeight * (charWeight[beginCharWeight[i + 1] - 1] + 1);
      if (midWeight > secondBucket[0]) {
        midWeight = midWeight / (charWeight[beginCharWeight[i + 1] - 1] + 1);
        sortSecondNumber[0] = i - sortCharNumber[0];
        break;
      }
    }
    secondBucket[0] = midWeight;
  }

}
