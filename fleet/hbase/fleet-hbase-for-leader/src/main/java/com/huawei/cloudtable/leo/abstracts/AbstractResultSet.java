package com.huawei.cloudtable.leo.abstracts;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public abstract class AbstractResultSet implements ResultSet {

  // Unsupported Method*************************************

  @Override
  public final String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void setFetchSize(final int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void setFetchDirection(final int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean isFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final String getNString(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final String getNString(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final URL getURL(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final URL getURL(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final RowId getRowId(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final RowId getRowId(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getBinaryStream(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getBinaryStream(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Reader getCharacterStream(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Reader getCharacterStream(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Reader getNCharacterStream(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Reader getNCharacterStream(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Blob getBlob(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Blob getBlob(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Clob getClob(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Clob getClob(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final NClob getNClob(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final NClob getNClob(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Array getArray(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Array getArray(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final SQLXML getSQLXML(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final SQLXML getSQLXML(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Ref getRef(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final Ref getRef(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getAsciiStream(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getAsciiStream(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getUnicodeStream(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final InputStream getUnicodeStream(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNull(final int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateByte(final int columnIndex, final byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateShort(final int columnIndex, final short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateInt(final int columnIndex, final int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateLong(final int columnIndex, final long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateFloat(final int columnIndex, final float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateDouble(final int columnIndex, final double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateString(final int columnIndex, final String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateDate(final int columnIndex, final Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateTime(final int columnIndex, final Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateObject(final int columnIndex, final Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNull(final String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateByte(final String columnLabel, final byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateShort(final String columnLabel, final short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateInt(final String columnLabel, final int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateLong(final String columnLabel, final long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateFloat(final String columnLabel, final float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateDouble(final String columnLabel, final double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateString(final String columnLabel, final String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();

  }

  @Override
  public final void updateDate(final String columnLabel, final Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateTime(final String columnLabel, final Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final String columnLabel, final Reader reader, final int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateObject(final String columnLabel, final Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateRef(final int columnIndex, final Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateRef(final String columnLabel, final Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final int columnIndex, final Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final String columnLabel, final Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final int columnIndex, final Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final String columnLabel, final Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateArray(final int columnIndex, final Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateArray(final String columnLabel, final Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateRowId(final int columnIndex, final RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateRowId(final String columnLabel, final RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNString(final int columnIndex, final String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNString(final String columnLabel, final String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final String columnLabel, final NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNCharacterStream(final String columnLabel, final Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final String columnLabel, final InputStream x, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final String columnLabel, final InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final String columnLabel, final Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final int columnIndex, final InputStream inputStream, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final String columnLabel, final InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final int columnIndex, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateClob(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public final void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}
