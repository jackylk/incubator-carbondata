package com.huawei.cloudtable.leo.value;

import com.huawei.cloudtable.leo.ValueCodecException;

import java.io.Serializable;

public final class Time implements Comparable<Time>, Serializable {

  private static final long serialVersionUID = -5047789829175145756L;

  public static Time of(final long time) {
    return new Time(new java.sql.Time((time % 86400000) / 1000 * 1000));
  }

  public static Time of(final String string) throws ValueCodecException {
    return new Time(java.sql.Time.valueOf(string));
  }

  private Time(final java.sql.Time value) {
    this.value = value;
  }

  private final java.sql.Time value;

  public long getTime() {
    return this.value.getTime();
  }

  @Override
  public int compareTo(final Time that) {
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
    if (object instanceof Time) {
      return this.value.equals(((Time)object).value);
    }
    return false;
  }

}