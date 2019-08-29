package com.huawei.cloudtable.leo.value;

import com.huawei.cloudtable.leo.ValueCodecException;

import java.io.Serializable;
import java.util.Calendar;

public final class Date implements Comparable<Date>, Serializable {

  private static final long serialVersionUID = 2888417981254835800L;

  private static final long timeZoneOffSetMS = Calendar.getInstance().get(Calendar.ZONE_OFFSET);

  public static Date of(final long time) {
    return new Date(new java.sql.Date((time + timeZoneOffSetMS) / 86400000 * 86400000 - timeZoneOffSetMS));
  }

  public static Date of(final String string) throws ValueCodecException {
    return new Date(java.sql.Date.valueOf(string));
  }

  private Date(final java.sql.Date value) {
    this.value = value;
  }

  private final java.sql.Date value;

  public long getTime() {
    return this.value.getTime();
  }

  @Override
  public int compareTo(final Date that) {
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
    if (object instanceof Date) {
      return this.value.equals(((Date)object).value);
    }
    return false;
  }

}
