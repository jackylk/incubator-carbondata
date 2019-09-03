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

abstract class GetSortValue<K> {

  /**
   * Return total of no dict sort column number
   */
  abstract int getSortColumnsNumber();

  /**
   * Return length of first nodict sort column
   */
  abstract void initSortColumnsLength(int[] maxLength, int[] minLength, int maxStringLength);

  /**
   * get the bucket number
   */
  abstract int getBucketValue(K row, int[] charWeight, int[] beginCharWeight, int beginNumber,
                              int sortCharNumber, int[] weightFinal, int[] minByte);

  /**
   * byte distribution range
   */
  abstract void byteDistributionRange(K row, int[] maxLength, int[] minLength, int[] learnFirst,
                                      int maxStringLength);

  /**
   * Return whether has null data
   */
  abstract boolean getIsHaveNull();
}