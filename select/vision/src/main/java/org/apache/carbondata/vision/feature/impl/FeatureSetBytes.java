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

package org.apache.carbondata.vision.feature.impl;

import org.apache.carbondata.vision.feature.FeatureSet;

public class FeatureSetBytes implements FeatureSet<byte[]> {

  private byte[] values;

  private int length;
  private int offset;

  @Override public void init(long capacity) {
    values = new byte[(int)capacity];
    this.length = 0;
    this.offset = 0;
  }

  @Override public void addFeature(byte[] feature) {
    System.arraycopy(feature, 0, values, offset, feature.length);
    length++;
    offset += feature.length;
  }

  @Override public void finish() {

  }

  @Override public byte[] getValues() {
    return values;
  }

  @Override public void setValues(byte[] values) {
    this.values = values;
  }

  @Override public int getLength() {
    return length;
  }

  @Override public void setLength(int length) {
    this.length = length;
  }

  @Override public long getAddress() {
    return 0;
  }

  @Override public byte[] getBytes() {
    return values;
  }
}
