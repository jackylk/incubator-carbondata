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
package org.apache.carbondata.processing.loading.partition.learned;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.model.learned.LearnedPartitionModel;
import org.apache.carbondata.processing.loading.partition.impl.RawRowComparator;
import org.apache.carbondata.processing.loading.sort.learned.LearnedDistribution;
import org.apache.carbondata.processing.loading.sort.learned.SerializableLearnedDistribution;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

public class LearnedPartitionAlgorithm implements PartitionAlgorithm {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LearnedPartitionAlgorithm.class.getName());

  private static double ERROR_BAND = 0.2;

  private static double ERROR_LOW_BAND = 0.001;

  private static int UNIT_LENGTH_DOUBLE_BYTE = 65536;

  private int supportMaxLength;

  //the number of sort columns;
  private int numberOfNoDictSortColumns;

  //the number of partition
  private int partitionNumber;

  //The min length of data
  private int[] minLength;

  //The max length of data
  private int[] maxLength;

  //The real length of double byte
  private int[] minLengthDoubleByte;

  //The real length of double byte
  private int[] maxLengthDoubleByte;

  //Number of data null
  private int[] nullNumberData;

  //the sortColumns for partition
  private int sortColumnsForPartion;

  //the byte of  sortcolumns for partion
  private int sortDoubleByteForPartion;

  private int partitionRange = 500000;

  //the byte of  sortcolumns for partion
  private int tableThreshold;

  //the byte weigh for partion
  private int[] multiWeight;

  //condense number
  private int shiftNumber;

  //the final partition table
  private short[] finalTable;

  private String columnsBoundsStr;

  private int[] byteMultiWeight;
  //Min byte
  private List<int[]> minDoubleByte = new ArrayList<>();

  //Max byte
  private List<int[]> maxDoubleByte = new ArrayList<>();

  //get weight table
  private List<int[]> weightTable = new ArrayList<>();

  //byte position in weightTable
  private List<int[]> bytePositionWeightTable = new ArrayList<>();

  private DataField[] fields;

  private CarbonRow[] samplesRowsList;

  private SerializableLearnedDistribution[] learnedDistribution;

  private int[] sortColumnIndex;

  private CarbonDataLoadConfiguration configuration;

  private boolean isUseSample;

  public LearnedPartitionAlgorithm(CarbonDataLoadConfiguration configuration) {
    this.configuration = configuration;
    this.numberOfNoDictSortColumns = configuration.getNumberOfNoDictSortColumns();
    this.partitionNumber = configuration.getNumberOfRangePartition();
    this.tableThreshold = partitionNumber * partitionRange;
    this.sortColumnIndex = configuration.getSortColumnRangeInfo().getSortColumnIndex();
    //init model
    this.minLength = new int[numberOfNoDictSortColumns];
    this.maxLength = new int[numberOfNoDictSortColumns];
    this.nullNumberData = new int[numberOfNoDictSortColumns];
    this.fields = configuration.getDataFields();
  }

  @Override public LearnedPartitionModel getLearnedPartitionModel() {
    if (isUseSample) {
      return null;
    }
    LearnedPartitionModel learnedPartitionModel = new LearnedPartitionModel();
    learnedPartitionModel.setModelMinDoubleByte(minDoubleByte);
    learnedPartitionModel.setModelWeightDoubleByte(weightTable);
    learnedPartitionModel.setModelBeginPosition(bytePositionWeightTable);
    learnedPartitionModel.setModelMultiplyWeight(multiWeight);
    learnedPartitionModel.setShiftNumber(shiftNumber);
    learnedPartitionModel.setFinalTable(finalTable);
    learnedPartitionModel.setSortColumnsForPartion(sortColumnsForPartion);
    learnedPartitionModel.setSortDoubleByteForPartion(sortDoubleByteForPartion);
    learnedPartitionModel.setbyteMultiWeight(byteMultiWeight);
    return learnedPartitionModel;
  }

  @Override public String getSortColumnsBoundsStr() {
    return columnsBoundsStr;
  }

  @Override public void learn(DistributionHolder[] distributionHolders) {
    if (!learnInternal(distributionHolders)) {
      // sampled partition
      learnWithSamples();
      isUseSample = true;
    }
  }

  private boolean learnInternal(DistributionHolder[] distributionHolders) {
    List<short[][]> learnDoubleByteDistribution = new ArrayList<>();
    List<int[]> tmpMinLength = new ArrayList<>();
    List<int[]> tmpMaxLength = new ArrayList<>();
    List<int[]> tmpNullNumberData = new ArrayList<>();
    List<CarbonRow> tmpSamplesList = new ArrayList<>();
    mergeLearnedData(distributionHolders, learnDoubleByteDistribution, tmpMinLength, tmpMaxLength,
        tmpNullNumberData, tmpSamplesList);

    samplesRowsList = new CarbonRow[tmpSamplesList.size()];
    tmpSamplesList.toArray(samplesRowsList);
    //If byte length too long,use sample
    if (!calculateMinMaxLength(tmpMinLength, tmpMaxLength)) {
      return false;
    }
    mergeNullData(tmpNullNumberData);
    short[][] learnDoubleByte = learnDoubleByteDistribution.get(0);
    minLengthDoubleByte = new int[numberOfNoDictSortColumns];
    maxLengthDoubleByte = new int[numberOfNoDictSortColumns];
    initByteMinMax(learnDoubleByte, learnDoubleByteDistribution);

    calculateMinMaxByte(learnDoubleByte);
    calculateByteWeight(learnDoubleByte);
    calculatePosition();
    calculateMultiWeight();
    calculateByteMultiWeight();
    if (!isFit()) {
      return false;
    }

    double[] probabilityTable = new double[tableThreshold];
    if (!adjustProbabilityTable(probabilityTable)) {
      return false;
    }

    calculateFinalTable(probabilityTable);
    antiModelForColumnsBounds(finalTable);
    return true;
  }

  private boolean isFit() {
    for (int i = 0; i < sortColumnsForPartion; i++) {
      if (nullNumberData[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private void mergeLearnedData(DistributionHolder[] distributionHolders,
      List<short[][]> learnDoubleByteDistribution, List<int[]> midMinLength,
      List<int[]> midMaxLength, List<int[]> midNullNumberData, List<CarbonRow> midSamplesList) {

    for (DistributionHolder distributionHolder : distributionHolders) {
      LearnedDistributionHolder learnedDistributionHolder =
          (LearnedDistributionHolder) distributionHolder;
      learnDoubleByteDistribution.add(learnedDistributionHolder.getLearnDoubleByte());
      midMinLength.add(learnedDistributionHolder.getMinLength());
      midMaxLength.add(learnedDistributionHolder.getMaxLength());
      midNullNumberData.add(learnedDistributionHolder.getNullNumberData());
      midSamplesList.addAll(learnedDistributionHolder.getSamplesList());
      supportMaxLength = learnedDistributionHolder.getSupportMaxLength();
    }
  }

  private void initByteMinMax(short[][] learnDoubleByte,
      List<short[][]> learnDoubleByteDistribution) {
    for (int i = 1; i < learnDoubleByteDistribution.size(); i++) {
      for (int j = 0; j < learnDoubleByteDistribution.get(i).length; j++) {
        int ArrayLength = learnDoubleByteDistribution.get(i)[j].length;
        short[][] midArrLearnDoubleByteDistribution = learnDoubleByteDistribution.get(i);
        for (int k = 0; k < ArrayLength; k++) {
          learnDoubleByte[j][k] =
              (short) (learnDoubleByte[j][k] | midArrLearnDoubleByteDistribution[j][k]);
        }
      }
    }

    for (int i = 0; i < numberOfNoDictSortColumns; i++) {
      int[] midMin = new int[(int) Math.ceil((float) maxLength[i] / 2)];
      minDoubleByte.add(midMin);
      int[] midMax = new int[(int) Math.ceil((float) maxLength[i] / 2)];
      maxDoubleByte.add(midMax);
      minLengthDoubleByte[i] = (int) Math.ceil((float) minLength[i] / 2);
      maxLengthDoubleByte[i] = (int) Math.ceil((float) maxLength[i] / 2);
    }
  }

  private void calculateByteMultiWeight() {
    byteMultiWeight = new int[sortColumnsForPartion];
    for (int i = 1; i < sortColumnsForPartion; i++) {
      byteMultiWeight[i] = maxLengthDoubleByte[i - 1] + byteMultiWeight[i - 1];
    }
  }

  private void antiModelForColumnsBounds(short[] finalTable) {
    StringBuilder sb = new StringBuilder();
    int temp = 0;
    for (int i = 1; i < finalTable.length; i++) {
      if (finalTable[i] != temp) {
        sb.append(calculateStrBound(i - 1)).append(";");
        temp++;
      }
    }
    if (sb.length() != 0) {
      columnsBoundsStr = sb.substring(0, sb.length() - 1);
    } else {
      columnsBoundsStr = null;
    }

  }

  private String calculateStrBound(int num) {
    StringBuilder result = new StringBuilder();
    int oriNum = num * shiftNumber;

    for (int i = 0; i < sortColumnsForPartion; i++) {
      int[] midMinDoubleByte = minDoubleByte.get(i);
      int[] midBytePositionWeightTable = bytePositionWeightTable.get(i);
      int[] midResult = new int[midBytePositionWeightTable.length];
      if (i == (sortColumnsForPartion - 1)) {
        oriNum = antiBytes(oriNum, sortDoubleByteForPartion, i, midResult);
      } else {
        oriNum = antiBytes(oriNum, midBytePositionWeightTable.length, i, midResult);
      }
      // get sort column str
      result.append(learnedDistribution[i].antiForStr(midResult, midMinDoubleByte));
    }
    // padding
    for (int i = sortColumnsForPartion; i < numberOfNoDictSortColumns; i++) {
      result.append(0).append(",");
    }
    return result.substring(0, result.length() - 1);
  }

  private int antiBytes(int oriNum, int end, int colNumber, int[] midResult) {
    for (int j = 0; j < end; j++) {
      midResult[j] = oriNum / multiWeight[j + byteMultiWeight[colNumber]];
      midResult[j] = findReal(midResult[j], colNumber, j);
      oriNum = oriNum % (multiWeight[j + byteMultiWeight[colNumber]]);
    }
    return oriNum;
  }

  private int findReal(int midResult, int numSort, int numCol) {
    int result = 0;
    int[] midWeightTable = weightTable.get(numSort);
    int[] midByteWeightTable = bytePositionWeightTable.get(numSort);
    if (numCol == midByteWeightTable.length - 1) {
      int n = midWeightTable.length - midByteWeightTable[numCol];
      for (int i = 0; i < n; i++) {
        if (midWeightTable[i + midByteWeightTable[numCol]] == midResult) {
          result = i;
          return result;
        }
      }
    } else {
      int n = midByteWeightTable[numCol + 1] - midByteWeightTable[numCol];
      for (int i = 0; i < n; i++) {
        if (midWeightTable[i + midByteWeightTable[numCol]] == midResult) {
          result = i;
          return result;
        }
      }
    }

    return result;
  }

  private void calculateMinMaxByte(short[][] learnDoubleByte) {
    boolean flag;
    for (int i = 0; i < learnDoubleByte.length; i++) {
      short[] mid = learnDoubleByte[i];
      for (int j = 0; j < maxDoubleByte.get(i).length; j++) {
        flag = true;
        for (int k = 0; k < UNIT_LENGTH_DOUBLE_BYTE; k++) {
          if (0 != mid[j * UNIT_LENGTH_DOUBLE_BYTE + k]) {
            if (flag) {
              minDoubleByte.get(i)[j] = k;
              flag = false;
            }
            maxDoubleByte.get(i)[j] = k;
          }
        }
      }

      for (int m = minLengthDoubleByte[i]; m < maxLengthDoubleByte[i]; m++) {
        minDoubleByte.get(i)[m] = minDoubleByte.get(i)[m] - 1;
      }
    }
  }

  private boolean calculateMinMaxLength(List<int[]> midMinLength, List<int[]> midMaxLength) {
    System.arraycopy(midMinLength.get(0), 0, minLength, 0, minLength.length);
    System.arraycopy(midMaxLength.get(0), 0, maxLength, 0, maxLength.length);
    for (int i = 0; i < midMinLength.size(); i++) {
      int[] midMin = midMinLength.get(i);
      int[] midMax = midMaxLength.get(i);
      for (int j = 0; j < minLength.length; j++) {
        minLength[j] = minLength[j] <= midMin[j] ? minLength[j] : midMin[j];
        maxLength[j] = maxLength[j] >= midMax[j] ? maxLength[j] : midMax[j];
      }
    }
    for (int i = 0; i < minLength.length; i++) {
      if (minLength[i] > maxLength[i]) {
        return false;
      }
      if (supportMaxLength < maxLength[i]) {
        return false;
      }
    }
    return true;
  }

  private void mergeNullData(List<int[]> midNullNumberData) {
    for (int[] mid : midNullNumberData) {
      for (int j = 0; j < nullNumberData.length; j++) {
        nullNumberData[j] = nullNumberData[j] + mid[j];
      }
    }
  }

  private void calculateByteWeight(short[][] learnDoubleByte) {
    for (int i = 0; i < learnDoubleByte.length; i++) {
      int weightRange = 0;
      for (int j = 0; j < maxDoubleByte.get(i).length; j++) {
        weightRange = weightRange + maxDoubleByte.get(i)[j] - minDoubleByte.get(i)[j] + 1;
      }

      int[] subWeight = new int[weightRange];
      int subScriptWeight =
          doubleByteWeight(subWeight, learnDoubleByte[i], 0, minLengthDoubleByte[i],
              minDoubleByte.get(i), maxDoubleByte.get(i), 0, 0);
      doubleByteWeight(subWeight, learnDoubleByte[i], minLengthDoubleByte[i],
          maxLengthDoubleByte[i], minDoubleByte.get(i), maxDoubleByte.get(i), 1, subScriptWeight);

      weightTable.add(subWeight);
    }
  }

  private int doubleByteWeight(int[] subWeight, short[] tempArr, int begin, int end, int[] minByte,
      int[] maxByte, int initWeight, int subScriptWeight) {
    for (int j = begin; j < end; j++) {
      int weight = initWeight;
      if (weight == 1) {
        subScriptWeight++;     // positive null byte
      }
      for (int k = minByte[j] + initWeight; k <= maxByte[j]; k++) {
        if (0 != tempArr[j * UNIT_LENGTH_DOUBLE_BYTE + k]) {
          subWeight[subScriptWeight] = weight;
          weight++;
        }
        subScriptWeight++;
      }
    }
    return subScriptWeight;
  }

  private void calculatePosition() {
    for (int i = 0; i < weightTable.size(); i++) {
      int[] beginPosition = new int[maxLengthDoubleByte[i]];
      for (int j = 1; j < beginPosition.length; j++) {
        beginPosition[j] =
            beginPosition[j - 1] + maxDoubleByte.get(i)[j - 1] - minDoubleByte.get(i)[j - 1] + 1;
      }
      bytePositionWeightTable.add(beginPosition);
    }
  }

  private void calculateMultiWeight() {
    int[] totalWeight = new int[1];
    totalWeight[0] = 1;
    int[] byteLength = new int[1];
    confirmSortInfo(totalWeight, byteLength);

    if (totalWeight[0] <= tableThreshold) {
      sortColumnsForPartion = weightTable.size();
      sortDoubleByteForPartion = bytePositionWeightTable.get(weightTable.size() - 1).length;
      shiftNumber = 1;
      tableThreshold = totalWeight[0];
    } else {
      shiftNumber = (int) Math.ceil((float) totalWeight[0] / tableThreshold);
      tableThreshold = totalWeight[0] / shiftNumber;
    }

    multiWeight = new int[byteLength[0]];
    int subscript = byteLength[0];
    confirmMultiWeight(subscript);

  }

  private void confirmSortInfo(int[] totalWeight, int[] byteLength) {
    for (int i = 0; i < weightTable.size(); i++) {
      int[] tempArr = weightTable.get(i);
      if (confirmSortLimit(tempArr, i, totalWeight, byteLength)) {
        return;
      }
    }
  }

  private boolean confirmSortLimit(int[] mid, int colNumber, int[] totalWeight, int[] byteLength) {
    for (int j = 1; j < bytePositionWeightTable.get(colNumber).length; j++) {
      totalWeight[0] = totalWeight[0] * (mid[bytePositionWeightTable.get(colNumber)[j] - 1] + 1);
      byteLength[0]++;
      if (totalWeight[0] > tableThreshold) {
        sortColumnsForPartion = colNumber + 1;
        sortDoubleByteForPartion = j;
        return true;
      }
    }
    byteLength[0]++;
    totalWeight[0] = totalWeight[0] * (mid[mid.length - 1] + 1);
    if (totalWeight[0] > tableThreshold) {
      sortColumnsForPartion = colNumber + 1;
      sortDoubleByteForPartion = bytePositionWeightTable.get(colNumber).length;
      return true;
    }
    return false;
  }

  private void confirmMultiWeight(int subscript) {
    for (int i = sortDoubleByteForPartion; i > 0; i--) {
      if (i == sortDoubleByteForPartion) {
        subscript--;
        multiWeight[subscript] = 1;
      } else if (i == (bytePositionWeightTable.get(sortColumnsForPartion - 1).length - 1)) {
        subscript--;
        multiWeight[subscript] = multiWeight[subscript + 1] * (
            weightTable.get(sortColumnsForPartion - 1)[
                weightTable.get(sortColumnsForPartion - 1).length - 1] + 1);
      } else {
        subscript--;
        multiWeight[subscript] = multiWeight[subscript + 1] * (
            weightTable.get(sortColumnsForPartion - 1)[
                bytePositionWeightTable.get(sortColumnsForPartion - 1)[i + 1] - 1] + 1);
      }
    }

    for (int i = sortColumnsForPartion - 2; i >= 0; i--) {
      for (int j = maxLengthDoubleByte[i]; j > 0; j--) {
        if (j == maxLengthDoubleByte[i]) {
          subscript--;
          if (maxLengthDoubleByte[i + 1] != 1) {
            multiWeight[subscript] = multiWeight[subscript + 1] * (
                weightTable.get(i + 1)[bytePositionWeightTable.get(i + 1)[1] - 1] + 1);
          } else {
            multiWeight[subscript] = multiWeight[subscript + 1] * (
                weightTable.get(i + 1)[weightTable.get(i + 1).length - 1] + 1);
          }
        } else if (j == (bytePositionWeightTable.get(i).length - 1)) {
          subscript--;
          multiWeight[subscript] =
              multiWeight[subscript + 1] * (weightTable.get(i)[weightTable.get(i).length - 1] + 1);
        } else {
          subscript--;
          multiWeight[subscript] = multiWeight[subscript + 1] * (
              weightTable.get(i)[bytePositionWeightTable.get(i)[j + 1] - 1] + 1);
        }
      }
    }
  }

  private boolean adjustProbabilityTable(double[] probabilityTable) {
    learnedDistribution = new SerializableLearnedDistribution[sortColumnsForPartion];
    for (int i = 0; i < sortColumnsForPartion; i++) {
      learnedDistribution[i] =
          LearnedDistribution.getDistribution(fields[i].getColumn().getDataType());
    }
    int[] partitionTable = new int[probabilityTable.length];
    for (CarbonRow carbonRow : samplesRowsList) {
      partitionTable[getMapNumber(carbonRow)]++;
    }
    int sampleSize = samplesRowsList.length;
    double meanPro = (1.0 / partitionNumber) * ERROR_BAND * 2;
    double maxPro = 0;
    for (int i = 0; i < probabilityTable.length; i++) {
      probabilityTable[i] = (double) partitionTable[i] / sampleSize;
      maxPro = maxPro < probabilityTable[i] ? probabilityTable[i] : maxPro;
    }

    return maxPro <= meanPro;
  }

  private void calculateFinalTable(double[] probabilityTable) {
    finalTable = new short[tableThreshold];
    double meanPro = 1.0 / partitionNumber;
    double[] meanProbability = new double[partitionNumber];
    for (int i = 0; i < meanProbability.length; i++) {
      meanProbability[i] = meanPro * (i + 1);
    }
    double sum = 0;
    int par = 0;
    int rec = 0;
    for (int i = 0; i < probabilityTable.length; i++) {
      if ((Math.abs((sum - meanProbability[par]) / meanPro) < ERROR_LOW_BAND) || ((sum
          > meanProbability[par]))) {
        if ((Math.abs((sum - meanProbability[par]) / meanPro) > ERROR_BAND)) {
          i--;
          sum = sum - probabilityTable[i];
        }
        for (int j = rec; j <= i; j++) {
          finalTable[j] = (short) par;
        }
        par++;
        rec = i + 1;
        if (par == (partitionNumber - 1)) {
          for (int j = rec; j < probabilityTable.length; j++) {
            finalTable[j] = (short) par;
          }
          break;
        }
      } else if (i == (probabilityTable.length - 1)) {
        for (int j = rec; j <= i; j++) {
          finalTable[j] = (short) par;
        }
      }
      sum = sum + probabilityTable[i];
    }
  }

  /**
   * learned partition
   */
  private int getMapNumber(CarbonRow key) {
    int result = 0;
    int i = 0;
    int mid;
    for (int colIdx : sortColumnIndex) {
      if (i != (sortColumnsForPartion - 1)) {
        mid = learnedDistribution[i]
            .getSortPartition(key.getObject(colIdx), minDoubleByte.get(i), weightTable.get(i),
                bytePositionWeightTable.get(i), multiWeight, byteMultiWeight[i]);
      } else {
        mid = learnedDistribution[i]
            .getSortPartition(key.getObject(colIdx), minDoubleByte.get(i), weightTable.get(i),
                bytePositionWeightTable.get(i), multiWeight, byteMultiWeight[i],
                sortDoubleByteForPartion);
      }
      i++;
      result += mid;
    }
    return result / shiftNumber;
  }

  private void learnWithSamples() {
    // sort samples
    Arrays.sort(samplesRowsList,
        new RawRowComparator(configuration.getSortColumnRangeInfo().getSortColumnIndex(),
            configuration.getSortColumnRangeInfo().getIsSortColumnNoDict(), CarbonDataProcessorUtil
            .getNoDictDataTypes(configuration.getTableSpec().getCarbonTable())));

    // partition with boundsGap
    int boundsGap = samplesRowsList.length / configuration.getNumberOfRangePartition();
    ArrayList<CarbonRow> boundsList = new ArrayList<>();
    for (int i = 1; i < configuration.getNumberOfRangePartition(); i++) {
      boundsList.add(samplesRowsList[i * boundsGap]);
    }

    columnsBoundsStr = getSampleBoundsStr(boundsList);
  }

  private String getSampleBoundsStr(ArrayList<CarbonRow> boundsList) {
    if (boundsList.isEmpty()) {
      LOGGER.warn("Sampled partition may be failed, columns bounds list is empty.");
      return null;
    }
    SortColumnRangeInfo sortColumnRangeInfo = configuration.getSortColumnRangeInfo();
    // As the boundsList right now has both sort columns and non-sort columns, we should
    // delete the non-sort columns
    StringBuilder columnsBoundsStr = new StringBuilder();
    CarbonRow tmpObject;
    for (CarbonRow aBoundsList : boundsList) {
      tmpObject = aBoundsList;
      StringBuilder tmpBoundStr = new StringBuilder();
      int noDicIdx = 0;
      int i = 0;
      // get the bounds string based on DataType
      DataType[] noDicDataTypes =
          CarbonDataProcessorUtil.getNoDictDataTypes(configuration.getTableSpec().getCarbonTable());
      for (int colIdx : sortColumnRangeInfo.getSortColumnIndex()) {
        if (sortColumnRangeInfo.getIsSortColumnNoDict()[i]) {
          if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[noDicIdx])) {
            // for no dictionary numeric column get comparator based on the data type
            tmpBoundStr.append(tmpObject.getData()[colIdx]).append(",");
          } else {
            tmpBoundStr
                .append(new String((byte[]) (tmpObject.getData()[colIdx]), StandardCharsets.UTF_8))
                .append(",");
          }
          noDicIdx++;
        } else {
          tmpBoundStr.append(tmpObject.getData()[colIdx]).append(",");
        }
        i++;
      }
      columnsBoundsStr.append(tmpBoundStr.substring(0, tmpBoundStr.length() - 1)).append(";");
    }
    return columnsBoundsStr.substring(0, columnsBoundsStr.length() - 1);
  }

}
