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

package org.apache.carbondata.vision.algorithm;

import org.apache.carbondata.vision.common.VisionUtil;
import org.apache.carbondata.vision.predict.PredictContext;

public class AlgorithmExecutorFactory {

  public static AlgorithmExecutor getAlgorithmExecutor(PredictContext context) {
    if (context.getAlgorithm().getAlgorithmClass() != null) {
      Object executor = VisionUtil.newInstanceByName(context.getAlgorithm().getAlgorithmClass(),
          new Class[] { PredictContext.class }, new Object[] { context });
      if (executor instanceof AlgorithmExecutor) {
        return (AlgorithmExecutor) executor;
      }
    }
    return null;
  }
}
