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
package org.apache.carbondata.processing.loading.partition.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.processing.loading.model.learned.LearnedPartitionModel;
import org.apache.carbondata.processing.loading.partition.Partitioner;
import org.apache.carbondata.processing.loading.sort.learned.LearnedDistribution;
import org.apache.carbondata.processing.loading.sort.learned.SerializableLearnedDistribution;

@InterfaceAudience.Internal
public class LearnedPartitionerImpl implements Partitioner<CarbonRow> {

  private int[] sortColumnIndex;

  private DataType[] noDicDataTypes;

  private SerializableLearnedDistribution[] learnedDistribution;

  //Min byte
  private List<int[]> minDoubleByte = new ArrayList<>();
  //get weight table
  private List<int[]> weightTable = new ArrayList<>();
  //byte position in weightTable
  private List<int[]> bytePositionWeightTable = new ArrayList<>();
  private int[] multiplyWeight;
  private int shiftNumber;
  private short[] finalTable;
  //the sortColumns for partition
  private int sortColumnsForPartion;
  //the byte of  sortcolumns for partion
  private int sortDoubleByteForPartion;
  private int[] byteMultiWeight;

  public LearnedPartitionerImpl(LearnedPartitionModel learnedPartitionModel, int[] sortColumnIndex,
                                DataType[] noDicDataTypes) {
    this.sortColumnIndex = sortColumnIndex;
    this.noDicDataTypes = noDicDataTypes;
    initLearnedDistribution(learnedPartitionModel);
  }

  private void initLearnedDistribution(LearnedPartitionModel learnedPartitionModel) {
    this.minDoubleByte = learnedPartitionModel.getModelMinDoubleByte();
    this.weightTable = learnedPartitionModel.getModelWeightTable();
    this.bytePositionWeightTable = learnedPartitionModel.getModelBytePositionWeightTable();
    this.multiplyWeight = learnedPartitionModel.getModelMultiplyWeight();
    this.shiftNumber = learnedPartitionModel.getShiftNumber();
    this.finalTable = learnedPartitionModel.getFinalTable();
    this.sortColumnsForPartion = learnedPartitionModel.getSortColumnsForPartion();
    this.sortDoubleByteForPartion = learnedPartitionModel.getSortDoubleByteForPartion();
    this.byteMultiWeight = learnedPartitionModel.getByteMultiWeight();
    learnedDistribution = new SerializableLearnedDistribution[sortColumnsForPartion];
    for (int i = 0; i < sortColumnsForPartion; i++) {
      learnedDistribution[i] = LearnedDistribution.getDistribution(noDicDataTypes[i]);
    }
  }

  /**
   * learned partition
   */
  @Override
  public int getPartition(CarbonRow key) {
    int result = 0;
    int i = 0;
    int mid;
    for (int colIdx : sortColumnIndex) {
      if (i != (sortColumnsForPartion - 1)) {
        mid = learnedDistribution[i]
                .getSortPartition(key.getObject(colIdx), minDoubleByte.get(i), weightTable.get(i),
                        bytePositionWeightTable.get(i), multiplyWeight, byteMultiWeight[i]);
      } else {
        mid = learnedDistribution[i]
                .getSortPartition(key.getObject(colIdx), minDoubleByte.get(i), weightTable.get(i),
                        bytePositionWeightTable.get(i), multiplyWeight, byteMultiWeight[i],
                        sortDoubleByteForPartion);
      }
      i++;
      result += mid;
    }
    return finalTable[result / shiftNumber];
  }

}