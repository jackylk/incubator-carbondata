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

import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class ByteArrayWrapper {

  private byte[] binary;

  private int offset;

  private int len;

  private Cell.Type type;

  public ByteArrayWrapper(byte[] binary, int offset, int len, Cell.Type type) {
    this.binary = binary;
    this.offset = offset;
    this.len = len;
    this.type = type;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteArrayWrapper that = (ByteArrayWrapper) o;
    return (type == that.type) && Bytes
        .equals(binary, offset, len, that.binary, that.offset, that.len);
  }

  @Override public int hashCode() {

    int result = Objects.hash(offset, len);
    result = 31 * result + Arrays.hashCode(binary);
    return result;
  }
}
