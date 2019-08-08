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

package org.apache.carbondata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.leo.LeoEnv;

public class LeoTest {

  private static SparkSession session;

  private static void init() {
    SparkSession.Builder builder = SparkSession.builder()
        .master("local")
        .config("spark.driver.host", "localhost")
        .config("fs.s3a.endpoint", "obs.cn-north-7.ulanqab.huawei.com:5443")
        .config("fs.s3a.access.key", "Q0T7MOHUY0KVLKMEEM3M")
        .config("fs.s3a.secret.key", "HWmgXbcIWu2333e2HVdjq19oD9AfJULAuEdRgyDO")
        .config("hive.warehouse.subdir.inherit.perms", false)
        .config("spark.sql.warehouse.dir", "s3a://leo/")
        .config("hive.exec.scratchdir", "s3a://leo/tmp/hive");

    session = LeoEnv.getOrCreateLeoSession(builder);
    session.sparkContext().setLogLevel("ERROR");
  }

  static Dataset<Row> sql(String sqlString) {
    if (session == null) {
      init();
    }
    return session.sql(sqlString);
  }

  static SparkSession getSession() {
    if (session == null) {
      init();
    }
    return session;
  }
}
