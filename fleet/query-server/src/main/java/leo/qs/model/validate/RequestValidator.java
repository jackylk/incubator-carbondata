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

package leo.qs.model.validate;

import org.apache.carbondata.core.util.CarbonProperties;

import leo.qs.exception.InvalidArgumentsException;
import leo.qs.model.view.SqlRequest;
import org.apache.commons.lang.StringUtils;

public class RequestValidator {

  public static void validateSql(SqlRequest request) throws InvalidArgumentsException {
    if (request == null) {
      throw new InvalidArgumentsException("Select should not be null");
    }
    if (StringUtils.isEmpty(request.getSqlStatement())) {
      throw new InvalidArgumentsException("sql statement is invalid");
    }
  }

  public static void validateOffsetLimit(int offset, int limit) throws InvalidArgumentsException {
    if (offset < 0 || offset > CarbonProperties.getInstance().getAsyncQueryResLimit()) {
      throw new InvalidArgumentsException("offset should be integer between 0 to " +
          CarbonProperties.getInstance().getAsyncQueryResLimit());
    }
    if (limit < 0 || limit > 1000) {
      throw new InvalidArgumentsException("offset should be integer between 0 to 1000.");
    }
    if (offset + limit > CarbonProperties.getInstance().getAsyncQueryResLimit()) {
      throw new InvalidArgumentsException("offset plus limit should be integer less than " +
          CarbonProperties.getInstance().getAsyncQueryResLimit());
    }
  }
}
