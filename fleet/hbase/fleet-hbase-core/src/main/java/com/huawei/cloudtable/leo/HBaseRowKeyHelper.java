package com.huawei.cloudtable.leo;

public final class HBaseRowKeyHelper {

  public static byte[] next(final byte[] rowKey) {
    if (rowKey.length == 0) {
      return new byte[]{0};
    } else {
      byte[] nextRowKey = rowKey.clone();
      int position = nextRowKey.length - 1;
      boolean allBitIsOne = true;
      do {
        if (nextRowKey[position] == (byte) 0xFF) {
          nextRowKey[position] = 0;
          position--;
        } else {
          allBitIsOne = false;
          nextRowKey[position] = (byte) (nextRowKey[position] + 1);// (byte)128=-128
          break;
        }
      } while (position >= 0);
      if (allBitIsOne) {
        nextRowKey = new byte[rowKey.length + 1];
        System.arraycopy(rowKey, 0, nextRowKey, 0, rowKey.length);
      }
      return nextRowKey;
    }
  }

  private HBaseRowKeyHelper() {
    // to do nothing.
  }

}
