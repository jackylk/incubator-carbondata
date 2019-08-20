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

package org.apache.carbondata.leo.queryserver.aop;

import java.util.Arrays;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LogAspect {
  private static final Logger LOGGER = LogServiceFactory.getLogService(LogAspect.class.getName());

  @Pointcut("execution(public * org.apache.carbondata.leo.queryserver.controller.*.*(..))")
  public void apiLog() {

  }

  @Around("apiLog()")
  public Object arround(ProceedingJoinPoint pjp) throws Throwable {
    String classMethod =
        pjp.getSignature().getDeclaringTypeName() + "." + pjp.getSignature().getName();
    LOGGER.info("Enter " + classMethod + ", args :" + Arrays.toString(pjp.getArgs()));
    try {
      Object o = pjp.proceed();
      LOGGER.info("Exit " + classMethod + ", return :" + o);
      return o;
    } catch (Throwable e) {
      LOGGER.error("Exit " + classMethod + " with exception:" + e.getMessage());
      throw e;
    }
  }

}
