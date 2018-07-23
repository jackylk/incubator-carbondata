/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.carbondata.spark.spark.secondaryindex;

import java.util.Comparator;

import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;

/**
 * This class is for comparing the two mdkeys in no kettle flow.
 */
public class RowComparatorWithOutKettle implements Comparator<Object[]> {

  /**
   * noDictionaryColMaping mapping of dictionary dimensions and no dictionary dimensions.
   */
  private boolean[] noDictionaryColMaping;

  /**
   * @param noDictionaryColMaping
   */
  public RowComparatorWithOutKettle(boolean[] noDictionaryColMaping) {
    this.noDictionaryColMaping = noDictionaryColMaping;
  }

  /**
   * Below method will be used to compare two mdkeys
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;
    int index = 0;
    int noDictionaryIndex = 0;
    int[] leftMdkArray = (int[]) rowA[0];
    int[] rightMdkArray = (int[]) rowB[0];
    byte[][] leftNonDictArray = (byte[][]) rowA[1];
    byte[][] rightNonDictArray = (byte[][]) rowB[1];
    for (boolean isNoDictionary : noDictionaryColMaping) {
      if (isNoDictionary) {
        diff = UnsafeComparer.INSTANCE
            .compareTo(leftNonDictArray[noDictionaryIndex], rightNonDictArray[noDictionaryIndex]);
        if (diff != 0) {
          return diff;
        }
        noDictionaryIndex++;
      } else {
        diff = leftMdkArray[index] - rightMdkArray[index];
        if (diff != 0) {
          return diff;
        }
        index++;
      }

    }
    return diff;
  }
}
