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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Algorithm implements Serializable, Writable {

  private String algorithmClass;
  private String algorithmVersion;
  private Map<String, String> properties;

  public Algorithm() {
  }

  public Algorithm(String algorithmClass, String algorithmVersion) {
    this.algorithmClass = algorithmClass;
    this.algorithmVersion = algorithmVersion;
  }

  public String getAlgorithmClass() {
    return algorithmClass;
  }

  public void setAlgorithmClass(String algorithmClass) {
    this.algorithmClass = algorithmClass;
  }

  public String getAlgorithmVersion() {
    return algorithmVersion;
  }

  public void setAlgorithmVersion(String algorithmVersion) {
    this.algorithmVersion = algorithmVersion;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, algorithmClass);
    WritableUtils.writeString(out, algorithmVersion);
  }

  @Override public void readFields(DataInput in) throws IOException {
    algorithmClass = WritableUtils.readString(in);
    algorithmVersion = WritableUtils.readString(in);
  }
}
