/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.carbondata.spark.spark.secondaryindex.exception;

/**
 * Exception class specific to SecondaryIndex creation
 */
public class SecondaryIndexException extends Exception {

  private String message;

  public SecondaryIndexException(String message) {
    super(message);
    this.message = message;
  }

  public SecondaryIndexException(String message, Throwable t) {
    super(message, t);
    this.message = message;
  }

  @Override public String getMessage() {
    return message;
  }
}
