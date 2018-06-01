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

package org.apache.carbondata.vision.cache;

import org.apache.carbondata.vision.common.VisionException;

public enum CacheLevel {

  Memory(0), Disk(1), NoCache(9);

  private int index;

  CacheLevel(int index) {
    this.index = index;
  }

  public static CacheLevel get(int index) throws VisionException {
    switch (index) {
      case 0:
        return Memory;
      case 1:
        return Disk;
      case 9:
        return NoCache;
      default:
        throw new VisionException("invalid cache level " + index);
    }
  }

  public int getIndex() {
    return index;
  }
}
