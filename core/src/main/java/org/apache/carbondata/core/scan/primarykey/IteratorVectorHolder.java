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

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.iterator.CarbonBatchIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

public class IteratorVectorHolder implements IteratorHolder {

  private CarbonColumnarBatch columnarBatch;

  private PrimaryKeyVectorComparator comparator;

  private CarbonBatchIterator iterator;

  private int counter = -1;

  private boolean firstTme;

  private BlockExecutionInfo executionInfo;

  private int deletedRows;

  public IteratorVectorHolder(PrimaryKeyVectorComparator comparator, CarbonBatchIterator iterator,
      CarbonColumnarBatch columnarBatch, BlockExecutionInfo executionInfo) {
    this.iterator = iterator;
    this.columnarBatch = columnarBatch;
    this.comparator = comparator;
    this.executionInfo = executionInfo;
  }

  public boolean hasNext() {

    if (!firstTme) {
      firstTme = true;
      if (!processData()) {
        return false;
      }
    }
    if (counter >= columnarBatch.getRowCounter() - 1 && iterator.hasNext()) {
      counter = -1;
      return processData();
    }
    return counter < columnarBatch.getRowCounter() - 1 || iterator.hasNext();
  }

  private boolean processData() {
    boolean hasData = false;
    while (iterator.hasNext()) {
      columnarBatch.reset();
      iterator.processNextBatch(columnarBatch);
      if (columnarBatch.getRowCounter() > 0) {
        hasData = true;
        break;
      }
    }
    return hasData;
  }

  public void read() {
    counter++;
  }

  @Override public Object getCell(int columnOrdinal) {
    return columnarBatch.columnVectors[columnOrdinal].getData(counter);
  }

  @Override public int compareTo(IteratorHolder o) {
    return comparator.compare(this, o);
  }

  public CarbonColumnarBatch getColumnarBatch() {
    return columnarBatch;
  }

  @Override public BlockExecutionInfo getBlockExecutionInfo() {
    return executionInfo;
  }

  @Override public boolean isDeleted() {
    return columnarBatch.getFilteredRows()[counter];
  }
}
