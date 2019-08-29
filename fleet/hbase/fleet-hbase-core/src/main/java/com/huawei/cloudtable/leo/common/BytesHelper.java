package com.huawei.cloudtable.leo.common;

public final class BytesHelper {

  public static final byte[] EMPTY_BYTES = new byte[0];

  public static byte invert(final byte aByte) {
    return (byte) (aByte ^ 0xFF);
  }

  public static byte[] invert(final byte[] bytes) {
    return invert(bytes, 0, bytes.length);
  }

  public static byte[] invert(final byte[] bytes, final int offset, int length) {
    final byte[] invertedBytes = new byte[length];
    invert(bytes, offset, invertedBytes, 0, length);
    return invertedBytes;
  }

  public static void invert(
      final byte[] sourceBytes,
      final int sourceOffset,
      final byte[] targetBytes,
      final int targetOffset,
      final int length
  ) {
    for (int i = 0; i < length; i++) {
      targetBytes[targetOffset + i] = (byte) (sourceBytes[sourceOffset + i] ^ 0xFF);
    }
  }

  public static int compare(final byte[] bytes1, final byte[] bytes2) {
    final int length = Math.min(bytes1.length, bytes2.length);
    for (int index = 0; index < length; index++) {
      final int compareResult = compare(bytes1[index], bytes2[index]);
      if (compareResult != 0) {
        return compareResult;
      }
    }
    return bytes1.length - bytes2.length;
  }

  public static boolean equals(final byte[] bytes1, final int offset1, final int length1, final byte[] bytes2, final int offset2, final int length2) {
    if (length1 != length2) {
      return false;
    }
    int index = 0;
    for (; index < length1; index++) {
      if (bytes1[offset1 + index] != bytes2[offset2 + index]) {// TODO 一个个字节比较有点慢
        return false;
      }
    }
    return true;
  }

  public static int compare(final byte[] bytes1, final int offset1, final int length1, final byte[] bytes2, final int offset2, final int length2) {
    final int length = Math.min(length1, length2);
    int index = 0;
    for (; index < length; index++) {
      final int compareResult = compare(bytes1[offset1 + index], bytes2[offset2 + index]);// TODO 一个个字节比较有点慢
      if (compareResult != 0) {
        return compareResult;
      }
    }
    return (length1 - index) - (length2 - index);
  }

  public static int compare(final byte byte1, final byte byte2) {
    return Byte.toUnsignedInt(byte1) - Byte.toUnsignedInt(byte2);// TODO 转成int，效率较低
  }

  public static String toHexString(final byte aByte) {
    return toHexString(aByte, true);
  }

  public static String toHexString(final byte aByte, final boolean upperCase) {
    // TODO
    throw new UnsupportedOperationException();
  }

  private BytesHelper() {
    // to do nothing.
  }

}
