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

package org.apache.carbondata.spark.acl;

import java.io.File;

import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

import org.apache.hadoop.fs.Path;

/**
 * Helps to get Store content paths.
 */
public class CarbonBadRecordStorePath extends Path {

  private String badStorePath;

  public CarbonBadRecordStorePath(String storePathString) {
    super(storePathString);
    this.badStorePath = storePathString;
  }

  /**
   * returns the bad store path till table
   *
   * @param carbonTableIdentifier
   * @return
   */
  public String getCarbonBadRecordStoreTablePath(CarbonTableIdentifier carbonTableIdentifier) {
    return badStorePath + File.separator + carbonTableIdentifier.getDatabaseName() + File.separator
        + carbonTableIdentifier.getTableName();
  }

  /**
   * returns bad store path til database
   *
   * @param carbonTableIdentifier
   * @return
   */
  public String getCarbonBadRecordStoreDatabasePath(CarbonTableIdentifier carbonTableIdentifier) {
    return badStorePath + File.separator + carbonTableIdentifier.getDatabaseName();
  }

  @Override public boolean equals(Object o) {
    if (null == o) {
      return false;
    }
    if (!(o instanceof CarbonBadRecordStorePath)) {
      return false;
    }
    CarbonBadRecordStorePath path = (CarbonBadRecordStorePath) o;
    return badStorePath.equals(path.badStorePath) && super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode() + badStorePath.hashCode();
  }

}
