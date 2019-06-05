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

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.util.Bytes;

public class QualifierArray {

  private byte[] cfBytes;

  private byte[] qualBytes;

  private int cfOffset;

  private int cfLen;

  private int qualOffset;

  private int qualLen;

  public QualifierArray(byte[] cfBytes, int cfOffset, int cfLen,byte[] qualBytes,
      int qualOffset, int qualLen) {
    this.cfBytes = cfBytes;
    this.cfOffset = cfOffset;
    this.cfLen = cfLen;
    this.qualBytes = qualBytes;
    this.qualOffset = qualOffset;
    this.qualLen = qualLen;
  }

  public QualifierArray() {
  }

  public void set(byte[] cfBytes, int cfOffset, int cfLen,byte[] qualBytes,
      int qualOffset, int qualLen) {
    this.cfBytes = cfBytes;
    this.cfOffset = cfOffset;
    this.cfLen = cfLen;
    this.qualBytes = qualBytes;
    this.qualOffset = qualOffset;
    this.qualLen = qualLen;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QualifierArray that = (QualifierArray) o;

    return Bytes.equals(cfBytes, cfOffset, cfLen, that.cfBytes, that.cfOffset, that.cfLen)
        && Bytes
        .equals(qualBytes, qualOffset, qualLen, that.qualBytes, that.qualOffset, that.qualLen);
  }

  @Override public int hashCode() {
    int result = Bytes.hashCode(cfBytes, cfOffset, cfLen);
    result = 31 * result + Bytes.hashCode(qualBytes, qualOffset, qualLen);
    return result;
  }


}
