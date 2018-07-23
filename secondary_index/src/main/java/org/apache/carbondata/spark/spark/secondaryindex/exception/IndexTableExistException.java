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
 *
 */
public class IndexTableExistException extends Exception {


  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The Error message.
   */
  private String msg = "";

  /**
   * Constructor
   *
   * @param msg The error message for this exception.
   */
  public IndexTableExistException(String msg) {
    super(msg);
    this.msg = msg;
  }

  /**
   * getMessage
   */
  @Override public String getMessage() {
    return this.msg;
  }
}
