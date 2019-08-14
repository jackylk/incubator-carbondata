/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package leo.qs.exception;

public enum ErrorCode {
  //success
  SUCCESS(0, "Success"),
  /* internal error code */
  ERROR(1, "Leo leader response error"),
  INTERNAL_ERROR(500, "Internal server error"),


  /* Error return by API */
  INVALID_PARAMETER(1001, "Invalid Parameters"),
  UNSUPPORTED_OP_ERROR(1002, "Op sql is not supported by leo"),
  JOB_STILL_RUNNING_ERROR(1003, "The job is still running, can not get result"),
  JOB_FAILED_ERROR(1004, "The job is failed, can not get result"),
  JOB_NOT_FOUND_ERROR(1005, "The job does not exist, can not get status or result"),
  JOB_NO_RESULT_SHOW_ERROR(1006, "The job does not have result to show."),


  LEO_ANALYSIS_ERROR(2001, "AnalysisException"),

  /**
   * error code for consumer
   */
  TOPIC_NOT_EXISTS_ERROR(2010, "Topic not exists "),
  CONSUMER_ALREADY_EXISTS(2011, "Consumer already exists "),
  CREATE_SOURCE_TABLE_FAILED(2012, "Failed to create source table "),
  START_CONSUMER_FAILED(2013, "Failed to start consumer "),

  CONSUMER_NOT_EXISTS(2014, "Consumer not exists "),
  STOP_CONSUMER_FAILED(2015, "Failed to stop consumer "),
  DROP_SOURCE_TABLE_FAILED(2016, "Failed to drop source table ");

  private final String CREATE_CONSUMER_FAILED = "Create consumer failed,";
  private final String DROP_CONSUMER_FAILED = "Drop consumer failed,";
  private final String DESC_CONSUMER_FAILED = "Desc consumer failed,";
  private final String SHOW_CONSUMERS_FAILED = "Show consumers failed,";

  private Integer code;
  private String msg;

  ErrorCode(Integer code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  public Integer getCode() {
    return this.code;
  }

  public String getMsg() {
    return msg;
  }
}
