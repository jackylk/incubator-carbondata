package com.huawei.cloudtable.leo.expression;

final class FunctionMatchException extends Exception {

  private static final long serialVersionUID = -3617380439155086218L;

  FunctionMatchException() {
  }

  FunctionMatchException(final String message) {
    super(message);
  }

  FunctionMatchException(final String message, final Throwable cause) {
    super(message, cause);
  }

  FunctionMatchException(final Throwable cause) {
    super(cause);
  }

}
