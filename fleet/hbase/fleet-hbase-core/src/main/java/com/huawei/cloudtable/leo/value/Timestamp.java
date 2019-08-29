package com.huawei.cloudtable.leo.value;

import com.huawei.cloudtable.leo.ValueCodecException;

import java.io.Serializable;

public final class Timestamp implements Comparable<Timestamp>, Serializable {

  private static final long serialVersionUID = -1040879733955877709L;

  public static Timestamp valueOf(final long time) {
    return new Timestamp(new java.sql.Timestamp(time));
  }

  public static Timestamp valueOf(String string) throws ValueCodecException {
    return new Timestamp(java.sql.Timestamp.valueOf(string));
  }

  private Timestamp(final java.sql.Timestamp value) {
    this.value = value;
  }

  private final java.sql.Timestamp value;

  public long getTime() {
    return this.value.getTime();
  }

  @Override
  public int compareTo(final Timestamp that) {
    return this.value.compareTo(that.value);
  }

  @Override
  public String toString() {
    return this.value.toString();
  }

  @Override
  public int hashCode() {
    return this.value.hashCode();
  }

  @Override
  public boolean equals(final Object object) {
    if (object ==this) {
      return true;
    }
    if (object instanceof Timestamp) {
      return this.value.equals(((Timestamp)object).value);
    }
    return false;
  }

}
