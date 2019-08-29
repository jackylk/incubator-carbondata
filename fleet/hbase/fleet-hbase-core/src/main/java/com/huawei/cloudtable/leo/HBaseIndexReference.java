package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.metadata.IndexDefinition;
import com.huawei.cloudtable.leo.metadata.IndexReference;

public final class HBaseIndexReference extends IndexReference {

  public HBaseIndexReference(final HBaseTableReference tableReference, final IndexDefinition indexDefinition) {
    super(tableReference, indexDefinition);
    this.tableReference = tableReference;
  }

  private final HBaseTableReference tableReference;

  @Override
  public HBaseTableReference getTableReference() {
    return this.tableReference;
  }

}
