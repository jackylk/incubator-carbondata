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
package org.apache.carbondata.processing.loading.model.learned;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LearnedPartitionModel implements Serializable {

  // Min byte
  private List<int[]> minDoubleByte = new ArrayList<>();

  // weight table
  private List<int[]> weightTable = new ArrayList<>();

  // byte position in weightTable
  private List<int[]> bytePositionWeightTable = new ArrayList<>();

  private int[] multiplyWeight;

  private int shiftNumber;

  private short[] finalTable;

  private int[] byteMultiWeight;

  //the sortColumns for partition
  private int sortColumnsForPartion;

  //the byte of  sortcolumns for partion
  private int sortDoubleByteForPartion;

  public void setModelMinDoubleByte(List<int[]> minDoubleByte) {
    this.minDoubleByte = minDoubleByte;
  }

  public void setModelWeightDoubleByte(List<int[]> weightTable) {
    this.weightTable = weightTable;
  }

  public void setModelBeginPosition(List<int[]> bytePositionWeightTable) {
    this.bytePositionWeightTable = bytePositionWeightTable;
  }

  public void setModelMultiplyWeight(int[] multiplyWeight) {
    this.multiplyWeight = multiplyWeight;
  }

  public void setShiftNumber(int shiftNumber) {
    this.shiftNumber = shiftNumber;
  }

  public void setFinalTable(short[] finalTable) {
    this.finalTable = finalTable;
  }

  public void setSortColumnsForPartion(int sortColumnsForPartion) {
    this.sortColumnsForPartion = sortColumnsForPartion;
  }

  public void setSortDoubleByteForPartion(int sortDoubleByteForPartition) {
    this.sortDoubleByteForPartion = sortDoubleByteForPartition;
  }

  public void setbyteMultiWeight(int[] byteMultiWeight) {
    this.byteMultiWeight = byteMultiWeight;
  }

  public List<int[]> getModelMinDoubleByte() {
    return minDoubleByte;
  }

  public List<int[]> getModelWeightTable() {
    return weightTable;
  }

  public List<int[]> getModelBytePositionWeightTable() {
    return bytePositionWeightTable;
  }

  public int[] getModelMultiplyWeight() {
    return multiplyWeight;
  }

  public int getShiftNumber() {
    return shiftNumber;
  }

  public short[] getFinalTable() {
    return finalTable;
  }

  public int getSortColumnsForPartion() {
    return sortColumnsForPartion;
  }

  public int getSortDoubleByteForPartion() {
    return sortDoubleByteForPartion;
  }

  public int[] getByteMultiWeight() {
    return byteMultiWeight;
  }

}
