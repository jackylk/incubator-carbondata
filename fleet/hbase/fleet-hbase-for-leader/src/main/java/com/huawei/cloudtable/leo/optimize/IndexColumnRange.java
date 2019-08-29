package com.huawei.cloudtable.leo.optimize;

import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.metadata.IndexDefinition;

final class IndexColumnRange {

  IndexColumnRange(final IndexDefinition.Column<?> indexColumn, final ValueRange<?> range) {
    this.indexColumn = indexColumn;
    this.range = range;
  }

  private final IndexDefinition.Column<?> indexColumn;

  private final ValueRange<?> range;

  IndexDefinition.Column<?> getIndexColumn() {
    return this.indexColumn;
  }

  ValueRange<?> getRange() {
    return this.range;
  }

}
