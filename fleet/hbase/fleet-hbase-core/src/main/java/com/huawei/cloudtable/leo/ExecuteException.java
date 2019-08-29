package com.huawei.cloudtable.leo;

public class ExecuteException extends Exception {

  private static final long serialVersionUID = -6092742900419529027L;

  public ExecuteException() {
  }

  public ExecuteException(final String message) {
    super(message);
  }

  public ExecuteException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public ExecuteException(final Throwable cause) {
    super(cause);
  }

}
