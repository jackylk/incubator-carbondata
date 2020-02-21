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

package org.apache.carbondata.core.datastore.page;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;

public class ByteBufferColumnPage extends VarLengthColumnPageBase {

  private ByteBuffer byteBuffer;

  ByteBufferColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    byteBuffer = ByteBuffer.allocateDirect((int)(pageSize * DEFAULT_ROW_SIZE * FACTOR));
  }

  @Override
  void putBytesAtRow(int rowId, byte[] bytes) {
    byteBuffer.put(bytes);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    // since it is variable length, we need to prepare each
    // element as LV result byte array (first two bytes are the length of the array)
    byte[] valueWithLength = addShortLengthToByteArray(bytes);

    // rowId * 4 represents the length of L in LV
    if (valueWithLength.length > (Integer.MAX_VALUE - totalLength - rowId * 4)) {
      // since we later store a column page in a byte array, so its maximum size is 2GB
      throw new RuntimeException("Carbondata only support maximum 2GB size for one column page,"
          + " exceed this limit at rowId " + rowId);
    }

    if (rowId == 0) {
      rowOffset.putInt(0, 0);
    }
    rowOffset.putInt(rowId + 1, rowOffset.getInt(rowId) + valueWithLength.length);
    putBytesAtRow(rowId, valueWithLength);
    totalLength += valueWithLength.length;
  }

  // Adds length as a short element (first 2 bytes) to the head of the input byte array
  private byte[] addShortLengthToByteArray(byte[] input) {
    if (input.length > Short.MAX_VALUE) {
      throw new RuntimeException("input data length " + input.length +
          " bytes too long, maximum length supported is " + Short.MAX_VALUE + " bytes");
    }
    byte[] output = new byte[input.length + 2];
    ByteBuffer buffer = ByteBuffer.wrap(output);
    buffer.putShort((short)input.length);
    buffer.put(input, 0, input.length);
    return output;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public byte[] getBytes(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    throw new UnsupportedOperationException("Operation not supported");
  }


}
