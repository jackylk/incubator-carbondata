package com.huawei.cloudtable.leo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class HBaseRowMutation {

  private HBaseRowMutation(final Map<Integer, ?> valueMapByColumn) {
    this.valueMapByColumn = valueMapByColumn;
  }

  private final Map<Integer, ?> valueMapByColumn;

  public boolean isEmpty() {
    return this.valueMapByColumn.isEmpty();
  }

  public Set<Integer> getColumnIndexes() {
    return this.valueMapByColumn.keySet();
  }

  public Object getColumnValue(final int columnIndex) {
    return this.valueMapByColumn.get(columnIndex);
  }

  public static final class Builder {

    public Builder() {
      this.valueMapByColumn = new HashMap<>();
    }

    private final Map<Integer, Object> valueMapByColumn;

    public void reset() {
      this.valueMapByColumn.clear();
    }

    public void setColumnValue(final int columnIndex, final Object columnValue) {
      this.valueMapByColumn.put(columnIndex, columnValue);
    }

    public HBaseRowMutation build() {
      return new HBaseRowMutation(new HashMap<>(this.valueMapByColumn));
    }

  }

}
