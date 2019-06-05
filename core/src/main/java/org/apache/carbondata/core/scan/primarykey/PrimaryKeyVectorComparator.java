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

package org.apache.carbondata.core.scan.primarykey;

import java.util.Comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PrimaryKeyVectorComparator implements Comparator<IteratorHolder> {

  private SerializableComparator[] comparators;

  private int[] sortOrdinals;

  public PrimaryKeyVectorComparator(DataType[] dataTypes, int[] sortOrdinals) {
    comparators = new SerializableComparator[dataTypes.length];
    this.sortOrdinals = sortOrdinals;
    for (int i = 0; i < dataTypes.length; i++) {
      comparators[i] =
          org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataTypes[i]);
    }
  }

  public PrimaryKeyVectorComparator(DataType[] dataTypes, int[] sortOrdinals,
      int timestampOrdinal, int deleteOrdinal) {
    comparators = new SerializableComparator[dataTypes.length + 2];
    this.sortOrdinals = new int[sortOrdinals.length + 1];
    System.arraycopy(sortOrdinals, 0, this.sortOrdinals, 0, sortOrdinals.length);
    this.sortOrdinals[this.sortOrdinals.length - 2] = timestampOrdinal;
    this.sortOrdinals[this.sortOrdinals.length - 1] = deleteOrdinal;
    for (int i = 0; i < dataTypes.length; i++) {
      comparators[i] =
          org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataTypes[i]);
    }
    comparators[this.sortOrdinals.length - 2] =
        org.apache.carbondata.core.util.comparator.Comparator.getComparator(DataTypes.LONG);
    comparators[this.sortOrdinals.length - 1] =
        org.apache.carbondata.core.util.comparator.Comparator.getComparator(DataTypes.LONG);
  }

  @Override public int compare(IteratorHolder o1, IteratorHolder o2) {
    int diff = 0;
    for (int i = 0; i < sortOrdinals.length; i++) {
      diff = comparators[i].compare(o1.getCell(sortOrdinals[i]), o2.getCell(sortOrdinals[i]));
      if (diff != 0) {
        return diff;
      }
    }
    return diff;
  }

}
