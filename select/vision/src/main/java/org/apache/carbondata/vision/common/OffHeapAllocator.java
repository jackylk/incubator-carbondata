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

package org.apache.carbondata.vision.common;

import java.lang.reflect.Field;

public class OffHeapAllocator {

  private static final int BYTE_ARRAY_OFFSET;

  private static sun.misc.Unsafe unsafe;

  static {
    try {
      Field cause = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      cause.setAccessible(true);
      unsafe = (sun.misc.Unsafe) cause.get(null);
    } catch (Throwable e) {
      unsafe = null;
    }

    if (unsafe == null) {
      BYTE_ARRAY_OFFSET = 0;
    } else {
      BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    }
  }

  public static long allocateMemory(long size) {
    return unsafe.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    unsafe.freeMemory(address);
  }

  public static void copy(byte[] values, long address) {
    unsafe.copyMemory(values, BYTE_ARRAY_OFFSET, null, address, values.length);
  }
}
