package com.huawei.cloudtable.leo;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

final class ResultSetMetadata implements ResultSetMetaData {

  ResultSetMetadata(final Table.Schema tableSchema, final List<String> tableAttributeLabelList) {
    this.tableSchema = tableSchema;
    this.tableAttributeLabelList = tableAttributeLabelList;
  }

  private final Table.Schema tableSchema;

  private final List<String> tableAttributeLabelList;
  
  public Table.Schema.Attribute<?> getAttribute(final int column) throws SQLException {
    try {
      return this.tableSchema.getAttribute(column - 1);
    } catch (IndexOutOfBoundsException exception) {
      throw ExceptionCode.COLUMN_INDEX_OUT_OF_BOUNDS.newException("Index: " + column);
    }
  }

  @Override
  public int getColumnCount() {
    return this.tableSchema.getAttributeCount();
  }

  @Override
  public String getColumnLabel(final int column) {
    return this.tableAttributeLabelList.get(column - 1);
  }

  @Override
  public String getColumnName(final int column) {
    return this.getColumnLabel(column);
  }

  @Override
  public int getColumnType(final int column) throws SQLException {
    final ColumnType columnType = ColumnType.get(this.getAttribute(column).getValueClass());
    return columnType == null ? Types.OTHER : columnType.getCode();
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException {
    final ColumnType columnType = ColumnType.get(this.getAttribute(column).getValueClass());
    if (columnType == null) {
      // TODO try UDT.
      throw new UnsupportedOperationException();
    } else {
      return columnType.getName().toString();
    }
  }

  @Override
  public String getColumnClassName(final int column) throws SQLException {
    return this.getAttribute(column).getValueClass().getName();
  }

  @Override
  public int getColumnDisplaySize(final int column) throws SQLException {
    // TODO
    final ColumnType columnType = ColumnType.get(this.getAttribute(column).getValueClass());
    return columnType == null ? 20 : columnType.getDefaultDisplaySize();
  }

  @Override
  public String getCatalogName(final int column) {
    return null;
  }

  @Override
  public String getSchemaName(final int column) {
    return null;
  }

  @Override
  public String getTableName(final int column) {
    return null;
  }

  @Override
  public int getPrecision(final int column) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public int getScale(final int column) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAutoIncrement(final int column) {
    return false;
  }

  @Override
  public boolean isCaseSensitive(final int column) {
    return true;
  }

  @Override
  public boolean isSearchable(final int column) {
    return true;
  }

  @Override
  public boolean isCurrency(final int column) {
    return false;
  }

  @Override
  public int isNullable(final int column) throws SQLException {
    return this.getAttribute(column).isNullable() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSigned(final int column) {
    return true;
  }

  @Override
  public boolean isReadOnly(final int column) {
    return true;
  }

  @Override
  public boolean isWritable(final int column) {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(final int column) {
    return false;
  }

  @Override
  public boolean isWrapperFor(final Class<?> clazz) {
    return clazz.isInstance(this);
  }

  @Override
  public final <T> T unwrap(final Class<T> clazz) throws SQLException {
    try {
      return clazz.cast(this);
    } catch (ClassCastException exception) {
      throw new SQLException(this.getClass().getName() + " not unwrapped from " + clazz.getName());
    }
  }

}
