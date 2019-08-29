package com.huawei.cloudtable.leo;

public class ValueCodecException extends IllegalArgumentException {

  private static final long serialVersionUID = 5368785883198505308L;

  public ValueCodecException() {
    super();
  }

  public ValueCodecException(final String message) {
    super(message);
  }

  public ValueCodecException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public ValueCodecException(final Throwable cause) {
    super(cause);
  }

}
