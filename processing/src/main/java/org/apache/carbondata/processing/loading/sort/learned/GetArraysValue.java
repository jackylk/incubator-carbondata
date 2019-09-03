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

public interface GetArraysValue<T> {

  void initSortColumnsLength(T row, int[] maxLength, int[] minLength, int maxStringLength);

  int getSortColumnsNumber(T row);

  void distributionRange(T row, int[] maxLength, int[] minLength, int[] learnFirst,
      int maxStringLength);

  int getLocateValue(T row, int[] charWeight, int[] beginCharWeight, int beginNumber, int endNumber,
      int[] weightFinal, int[] minByte);

  boolean getIsHaveNull();
}
