package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.abstracts.AbstractResultSet;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class ResultSet extends AbstractResultSet {

  private static final Logger LOGGER = Logger.getLogger(ResultSet.class);

  static final int TYPE = ResultSet.TYPE_FORWARD_ONLY;

  static final int CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

  static final int HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;

  static final int FETCH_DIRECTION = ResultSet.FETCH_FORWARD;

  ResultSet(final Table table, final List<String> columnLabelList) {
    this.table = table;
    this.columnLabelList = columnLabelList;
    this.columnIndexMapByLabel = null;
    this.metadata = null;
    this.cursorState = CursorState.BEFORE_FIRST_ROW;
    this.wasNull = false;
    this.closed = false;
  }

  private final Table table;

  private final List<String> columnLabelList;

  private Map<String, Integer> columnIndexMapByLabel;

  private ResultSetMetaData metadata;

  private CursorState cursorState;

  private boolean wasNull;

  private boolean closed;

  @Override
  public boolean next() {
    final boolean hasNext;
    try {
      hasNext = this.table.next();
    } catch (IOException exception) {
      // TODO
      throw new UnsupportedOperationException(exception);
    }
    this.cursorState = hasNext ? CursorState.NORMAL : CursorState.AFTER_LAST_ROW;
    return hasNext;
  }

  @Override
  public java.sql.Statement getStatement() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() {
    if (this.metadata == null) {
      this.metadata = new ResultSetMetadata(this.table.getSchema(), this.columnLabelList);
    }
    return this.metadata;
  }

  @Override
  public SQLWarning getWarnings() {
    return null;
  }

  @Override
  public int getType() {
    return TYPE;
  }

  @Override
  public int getConcurrency() {
    return CONCURRENCY;
  }

  @Override
  public int getHoldability() {
    return HOLDABILITY;
  }

  @Nullable
  private <TValue> TValue getValue(final int columnIndex, final Class<TValue> resultClass) throws SQLException {
    this.checkCursorState();
    final TValue value;
    try {
      value = this.table.get(columnIndex - 1, resultClass);
    } catch (IndexOutOfBoundsException exception) {
      throw new SQLException(exception);
    } catch (ClassCastException exception) {
      // TODO
      throw new UnsupportedOperationException();
    }
    this.wasNull = value == null;
    return value;
  }

  @Override
  public String getString(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, String.class);
  }

  @Override
  public String getString(final String columnLabel) throws SQLException {
    return this.getString(this.findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(final int columnIndex) throws SQLException {
    final Boolean value = this.getValue(columnIndex, Boolean.class);
    return value != null && value;
  }

  @Override
  public boolean getBoolean(final String columnLabel) throws SQLException {
    return this.getBoolean(this.findColumn(columnLabel));
  }

  @Override
  public byte getByte(final int columnIndex) throws SQLException {
    final Byte value = this.getValue(columnIndex, Byte.class);
    return value == null ? 0 : value;
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return this.getByte(this.findColumn(columnLabel));
  }

  @Override
  public short getShort(final int columnIndex) throws SQLException {
    final Short value = this.getValue(columnIndex, Short.class);
    return value == null ? 0 : value;
  }

  @Override
  public short getShort(final String columnLabel) throws SQLException {
    return this.getShort(this.findColumn(columnLabel));
  }

  @Override
  public int getInt(final int columnIndex) throws SQLException {
    final Integer value = this.getValue(columnIndex, Integer.class);
    return value == null ? 0 : value;
  }

  @Override
  public int getInt(final String columnLabel) throws SQLException {
    return this.getInt(this.findColumn(columnLabel));
  }

  @Override
  public long getLong(final int columnIndex) throws SQLException {
    final Long value = this.getValue(columnIndex, Long.class);
    return value == null ? 0 : value;
  }

  @Override
  public long getLong(final String columnLabel) throws SQLException {
    return this.getLong(this.findColumn(columnLabel));
  }

  @Override
  public final float getFloat(final int columnIndex) throws SQLException {
    final Float value = this.getValue(columnIndex, Float.class);
    return value == null ? 0 : value;
  }

  @Override
  public final float getFloat(final String columnLabel) throws SQLException {
    return this.getFloat(this.findColumn(columnLabel));
  }

  @Override
  public final double getDouble(final int columnIndex) throws SQLException {
    final Double value = this.getValue(columnIndex, Double.class);
    return value == null ? 0 : value;
  }

  @Override
  public final double getDouble(final String columnLabel) throws SQLException {
    return this.getDouble(this.findColumn(columnLabel));
  }

  @Override
  public final BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, BigDecimal.class);
  }

  @Override
  public final BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
    return this.getBigDecimal(this.findColumn(columnLabel));
  }

  @Override
  public byte[] getBytes(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, byte[].class);
  }

  @Override
  public byte[] getBytes(final String columnLabel) throws SQLException {
    return this.getBytes(this.findColumn(columnLabel));
  }

  @Override
  public Date getDate(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, Date.class);
  }

  @Override
  public Date getDate(final String columnLabel) throws SQLException {
    return this.getDate(this.findColumn(columnLabel));
  }

  @Override
  public Time getTime(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, Time.class);
  }

  @Override
  public Time getTime(final String columnLabel) throws SQLException {
    return this.getTime(this.findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(final int columnIndex) throws SQLException {
    return this.getValue(columnIndex, Timestamp.class);
  }

  @Override
  public Timestamp getTimestamp(final String columnLabel) throws SQLException {
    return this.getTimestamp(this.findColumn(columnLabel));
  }

  @Override
  public Object getObject(final int columnIndex) throws SQLException {
    try {
      final Object object = this.table.get(columnIndex - 1);
      this.wasNull = object == null;
      return object;
    } catch (IndexOutOfBoundsException exception) {
      throw new SQLException(exception);
    }
  }

  @Override
  public Object getObject(final String columnLabel) throws SQLException {
    return this.getObject(this.findColumn(columnLabel));
  }

  @Override
  public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
    try {
      return type.cast(this.getObject(columnIndex));
    } catch (NullPointerException exception) {
      if (type == null) {
        throw new SQLException(exception);
      } else {
        throw exception;
      }
    } catch (ClassCastException exception) {
      throw new SQLException(exception);
    }
  }

  @Override
  public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
    return this.getObject(this.findColumn(columnLabel), type);
  }

  @Override
  public int getFetchDirection() {
    return FETCH_DIRECTION;
  }

  @Override
  public int getFetchSize() {
    return 0;
  }

  @Override
  public boolean wasNull() {
    return this.wasNull;
  }

  @Override
  public int findColumn(final String columnLabel) throws SQLException {
    if (this.columnIndexMapByLabel == null) {
      final Map<String, Integer> columnIndexMapByLabel = new HashMap<>(this.columnLabelList.size());
      for (int index = 0; index < this.columnLabelList.size(); index++) {
        if (columnIndexMapByLabel.put(this.columnLabelList.get(index), index + 1) != null) {
          // TODO 标签名重复了
          throw new UnsupportedOperationException();
        }
      }
      this.columnIndexMapByLabel = columnIndexMapByLabel;
    }
    final Integer columnIndex = this.columnIndexMapByLabel.get(columnLabel);
    if (columnIndex == null) {
      throw new SQLException("Column [" + columnLabel + "] is not found in result set.");
    }
    return columnIndex;
  }

  @Override
  public boolean isBeforeFirst() {
    return false;
  }

  @Override
  public boolean isAfterLast() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public boolean isWrapperFor(final Class<?> clazz) {
    return clazz.isInstance(this);
  }

  @Override
  public void close() throws SQLException {
    if (this.closed) {
      return;
    }
    try {
      this.table.close();
    } catch (IOException exception) {
      LOGGER.warn(exception);
    }
  }

  @Override
  public final <T> T unwrap(final Class<T> clazz) throws SQLException {
    try {
      return clazz.cast(this);
    } catch (ClassCastException exception) {
      throw new SQLException(this.getClass().getName() + " not unwrapped from " + clazz.getName());
    }
  }

  private void checkOpenState() throws SQLException {
    if (this.closed) {
      throw ExceptionCode.RESULT_SET_CLOSED.newException();
    }
  }

  private void checkCursorState() throws SQLException {
    this.checkOpenState();
    switch (this.cursorState) {
      case BEFORE_FIRST_ROW:
        throw ExceptionCode.CURSOR_BEFORE_FIRST_ROW.newException();
      case NORMAL:
        return;
      case AFTER_LAST_ROW:
        throw ExceptionCode.CURSOR_AFTER_LAST_ROW.newException();
      default:
        throw new RuntimeException();
    }
  }

  private enum CursorState {

    BEFORE_FIRST_ROW,

    NORMAL,

    AFTER_LAST_ROW

  }

}
