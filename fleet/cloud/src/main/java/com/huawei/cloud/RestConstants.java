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

package com.huawei.cloud;

/**
 * It contains Cloud end points and rest url constants.
 */
public interface RestConstants {

  public static String HUAWEI_CLOUD_AUTH_ENDPOINT =
      "https://iam.cn-north-1.myhuaweicloud.com/v3/auth/tokens";

  public static String AUTH_TOKEN_HEADER = "X-Subject-Token";

  public static String MODELARTS_CN_NORTH_V1_ENDPOINT =
      "https://modelarts.cn-north-1.myhuaweicloud.com/v1/";

  public static String MODELARTS_TRAINING_REST = "training-jobs";

  public static String MODELARTS_TRAINING_VERSIONS = "versions";

  public static String SEPARATOR = "/";

}
