package com.huawei.cloudtable.leo;

public abstract class HBaseRowKeyDefinition {

  public abstract boolean isTheLastColumn(int columnIndex);

  public abstract boolean isColumnNullable(int columnIndex);

  public abstract int getColumnCount();

  public abstract int getColumnIndexInTable(int columnIndex);

  public abstract HBaseValueCodec getColumnCodec(int columnIndex);

}
