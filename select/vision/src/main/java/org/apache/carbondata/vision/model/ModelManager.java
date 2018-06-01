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

package org.apache.carbondata.vision.model;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.vision.common.VisionException;

public class ModelManager {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(ModelManager.class.getName());

  public static Model loadModel(String modelPath) throws VisionException {
    int result = loadKNNSearchModel(modelPath);
    LOGGER.audit("loaded Model: " + modelPath);
    if (result != 0) {
      throw new VisionException("Failed to load " + modelPath);
    }
    return new Model(modelPath, "1.0");
  }

  public static native int loadKNNSearchModel(String path);

}
