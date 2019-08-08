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

public class SparkS3Constants {
  public static final String AK = "spark.hadoop.fs.s3a.access.key";
  public static final String FS_S3_AK = "fs.s3a.access.key";
  public static final String SK = "spark.hadoop.fs.s3a.secret.key";
  public static final String FS_S3_SK = "fs.s3a.secret.key";
  public static final String END_POINT = "spark.hadoop.fs.s3a.endpoint";
  public static final String FS_S3_END_POINT = "fs.s3a.endpoint";

  public static final String OBS_AK = "spark.hadoop.fs.obs.access.key";
  public static final String FS_OBS_AK = "fs.obs.access.key";
  public static final String OBS_SK = "spark.hadoop.fs.obs.secret.key";
  public static final String FS_OBS_SK = "fs.obs.secret.key";
  public static final String OBS_END_POINT = "spark.hadoop.fs.obs.endpoint";
  public static final String FS_OBS_END_POINT = "fs.obs.endpoint";
}
