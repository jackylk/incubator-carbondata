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

import java.util.Iterator;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

public class IteratorRowHolder implements IteratorHolder {

  private Object[] currentRow;

  private Iterator<Object[]> iterator;

  private PrimaryKeyVectorComparator comparator;

  private BlockExecutionInfo executionInfo;

  private int deletedRows;

  public IteratorRowHolder(PrimaryKeyVectorComparator comparator, Iterator<Object[]> iterator,
      BlockExecutionInfo executionInfo) {
    this.iterator = iterator;
    this.comparator = comparator;
    this.executionInfo = executionInfo;
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public void read() {
    currentRow = iterator.next();
  }

  @Override public int compareTo(IteratorHolder o) {
    return comparator.compare(this, o);
  }

  @Override public Object getCell(int columnOrdinal) {
    return currentRow[columnOrdinal];
  }

  public Object[] getCurrentRow() {
    return currentRow;
  }

  @Override public BlockExecutionInfo getBlockExecutionInfo() {
    return executionInfo;
  }

  @Override public boolean isDeleted() {
    return false;
  }
}
