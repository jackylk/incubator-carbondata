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

package org.apache.carbondata.common.exceptions.sql;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

/**
 * This exception will be thrown if datamap is not found when executing datamap
 * related SQL statement
 */
@InterfaceAudience.User
@InterfaceStability.Stable
public class NoSuchDataMapException extends MalformedCarbonCommandException {

  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  public NoSuchDataMapException(String dataMapName, String tableName) {
    super("Datamap with name " + dataMapName + " does not exist under table " + tableName);
  }

  public NoSuchDataMapException(String dataMapName) {
    super("Datamap with name " + dataMapName + " does not exist");
  }
}
