package com.huawei.cloudtable.leo.optimize;

import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.expression.Evaluation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class IndexPrefixMatchResult {

  /**
   * @param scanInterval  前缀区间
   */
  IndexPrefixMatchResult(final Prefix prefix, final ValueRange.Interval<?> scanInterval) {
    this.prefix = prefix;
    this.scanInterval = scanInterval;
  }

  private final Prefix prefix;

  private final ValueRange.Interval<?> scanInterval;

  private Evaluation<Boolean> filterCondition;

  @Nonnull
  public Prefix getPrefix() {
    return this.prefix;
  }

  @Nonnull
  public ValueRange.Interval<?> getScanInterval() {
    return this.scanInterval;
  }

  @Nullable
  public Evaluation<Boolean> getFilterCondition() {
    return this.filterCondition;
  }

  void setFilterCondition(final Evaluation<Boolean> filterCondition) {
    this.filterCondition = filterCondition;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof IndexPrefixMatchResult) {
      final IndexPrefixMatchResult that = (IndexPrefixMatchResult)object;
      return this.prefix.equals(that.prefix)
          && this.scanInterval.equals(that.scanInterval)
          && this.filterCondition == null ? that.filterCondition == null : this.filterCondition.equals(that.filterCondition);
    }
    return false;
  }

  public static final class Prefix {

    static final Prefix EMPTY = new Prefix(new Comparable[0]);

    Prefix(final Comparable[] values) {
      this.values =values;
    }

    private final Comparable[] values;

    public int getDimension() {
      return this.values.length;
    }

    @Nonnull
    public Comparable get(final int dimensionIndex) {
      return this.values[dimensionIndex];
    }

    @Override
    public int hashCode() {
      if (this.values.length == 0) {
        return 0;
      }
      int hashCode = this.values[0].hashCode();
      for (int index = 1; index < this.values.length; index++) {
        hashCode = 31 * hashCode + this.values[index].hashCode();
      }
      return hashCode;
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object instanceof Prefix) {
        final Prefix that = (Prefix)object;
        if (this.values.length != that.values.length) {
          return false;
        }
        for (int index = 0; index < this.values.length; index++) {
          if (!this.values[index].equals(that.values[index])) {
            return false;
          }
        }
        return true;
      }
      return false;
    }

  }

}
