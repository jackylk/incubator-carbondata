package com.huawei.cloudtable.leo.expression;

public class FunctionDeclareException extends RuntimeException {

  private static final long serialVersionUID = 7973350703052821633L;

  public FunctionDeclareException() {
  }

  public FunctionDeclareException(final String message) {
    super(message);
  }

  public FunctionDeclareException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public FunctionDeclareException(final Throwable cause) {
    super(cause);
  }

}
