package com.huawei.cloudtable.leo;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;

import java.util.Arrays;

public abstract class HBaseRowKey {

  private static final HBaseRowKey EMPTY = new HBaseRowKey(new byte[0]) {
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(final Object object) {
      return object == this;
    }
  };

  public static HBaseRowKey empty() {
    return EMPTY;
  }

  public static HBaseRowKey of(final byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("Argument [bytes] is null.");
    }
    if (bytes.length == 0) {
      return EMPTY;
    } else {
      return new HBaseRowKey(bytes) {

        private int hashCode;

        @Override
        public int hashCode() {
          if (this.hashCode == 0) {
            int hashCode = 0;
            for (int index = 0; index < bytes.length; index++) {
              hashCode = 31 * hashCode + bytes[index];
            }
            this.hashCode = hashCode;
          }
          return this.hashCode;
        }

        @Override
        public boolean equals(final Object object) {
          if (object == this) {
            return true;
          }
          if (object instanceof HBaseRowKey) {
            return Arrays.equals(this.bytes, ((HBaseRowKey) object).bytes);
          } else {
            return false;
          }
        }

      };
    }
  }

  private HBaseRowKey(final byte[] bytes) {
    this.bytes = bytes;
  }

  final byte[] bytes;

  public Get newGet() {
    return new Get(this.bytes);
  }

  public Put newPut() {
    return new Put(this.bytes);
  }

  public Delete newDelete() {
    return new Delete(this.bytes);
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object object);

}
