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

package org.apache.carbondata.leo.queryserver.exception;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.carbondata.leo.queryserver.model.view.ErrorResult;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
public class RestExceptionHandler {
  private static Logger LOGGER =
      LogServiceFactory.getLogService(RestExceptionHandler.class.getCanonicalName());

  @ExceptionHandler(value = LeoServiceException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody public ErrorResult handleLeoServiceException(LeoServiceException e) {
    LOGGER.error(e.getMessage());
    return handle(e);
  }

  @ExceptionHandler(value = InvalidArgumentsException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody public ErrorResult handleInvalidArgumentsException(InvalidArgumentsException e) {
    LOGGER.error(e.getMessage());
    return handle(e);
  }

  @ExceptionHandler(value = UnsupportedOperationException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ErrorResult handleUnsupportedOperationException(UnsupportedOperationException e) {
    LOGGER.error(e.getMessage());
    return handle(e);
  }

  @ExceptionHandler(value = JobStatusException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ErrorResult handleJobStatusException(JobStatusException e) {
    LOGGER.error(e.getMessage());
    return handle(e);
  }

  @ExceptionHandler(value = FetchResultPagesException.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ResponseBody
  public ErrorResult handleFetchResultPagesException(FetchResultPagesException e) {
    LOGGER.error(e.getMessage());
    return handle(e);
  }

  private ErrorResult handle(Exception e) {
    if (e instanceof LeoServiceException) {
      LeoServiceException exception = (LeoServiceException) e;
      return new ErrorResult(exception.getCode(), exception.getMessage());
    } else if (e instanceof UnsupportedOperationException) {
      UnsupportedOperationException exception = (UnsupportedOperationException) e;
      return new ErrorResult(ErrorCode.UNSUPPORTED_OP_ERROR.getCode(), exception.getMessage());
    } else {
      LOGGER.error("Can not handle this error {}", e);
      return new ErrorResult(-1, "Unknown error");
    }
  }

}
