package com.huawei.cloudtable.leo;

import java.sql.SQLException;

public enum ExceptionCode {

  ILLEGAL_CONNECTION_URL(0, "Illegal connection url."),

  CONNECTION_CLOSED(1, "Connection is closed."),

  STATEMENT_CLOSED(2, "Statement is closed."),

  RESULT_SET_CLOSED(3, "Result set is closed."),

  CURSOR_BEFORE_FIRST_ROW(4, "Cursor is before the first row."),

  CURSOR_AFTER_LAST_ROW(5, "Cursor is after the last row."),

  COLUMN_INDEX_OUT_OF_BOUNDS(6, "Column index out build bounds.");

  ExceptionCode(final int code, final String message) {
    if (code < 0 || code > Short.MAX_VALUE) {
      throw new IllegalArgumentException();
    }
    this.string = "ERROR (" + Short.toString((short) code) + "): " + message;
  }

  private final String string;

  public SQLException newException() {
    return new SQLException(this.string);
  }

  public SQLException newException(final String message) {
    return new SQLException(this.string + " Message: " + message);
  }

  public SQLException newException(final Throwable cause) {
    return new SQLException(this.string, cause);
  }

  public SQLException newException(final String message, final Throwable cause) {
    return new SQLException(this.string + " Message: " + message, cause);
  }

}
