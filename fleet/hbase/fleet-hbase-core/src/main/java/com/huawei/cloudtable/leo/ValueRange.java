package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.common.CollectionHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 值域
 */
public abstract class ValueRange<TValue extends Comparable<TValue>> {

  @SuppressWarnings("unchecked")
  private static final ValueRange FULL = new ValueRange() {
    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean isFull() {
      return true;
    }

    @Override
    public boolean isPoint() {
      return false;
    }

    @Override
    public boolean isInfinite() {
      return true;
    }

    @Override
    public int getIntervalCount() {
      return 1;
    }

    @Override
    public Interval getInterval(final int index) {
      if (index != 0) {
        throw new IndexOutOfBoundsException();
      }
      return Interval.FULL;
    }

    @Override
    public boolean contains(final Comparable value) {
      return true;
    }

    @Override
    public ContainsResult contains(final Interval interval) {
      return ContainsResult.COMPLETELY;
    }
  };

  private static final ValueRange EMPTY = new ValueRange() {
    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean isFull() {
      return false;
    }

    @Override
    public boolean isPoint() {
      return false;
    }

    @Override
    public boolean isInfinite() {
      return false;
    }

    @Override
    public int getIntervalCount() {
      return 0;
    }

    @Override
    public Interval getInterval(final int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean contains(final Comparable value) {
      return false;
    }

    @Override
    public ContainsResult contains(final Interval interval) {
      return ContainsResult.NOT_CONTAINS;
    }
  };

  @SuppressWarnings("unchecked")
  public static <TValue extends Comparable<TValue>> ValueRange<TValue> full() {
    return FULL;
  }

  public static <TValue extends Comparable<TValue>> ValueRange<TValue> of(final TValue value) {
    if (value == null) {
      throw new IllegalArgumentException("Argument [value] is null.");
    }
    return of0(Interval.of(value));
  }

  public static <TValue extends Comparable<TValue>> ValueRange<TValue> of(
      final TValue minimum,
      boolean includeMinimum,
      final TValue maximum,
      boolean includeMaximum) {
    return of0(Interval.of(minimum, includeMinimum, maximum, includeMaximum));
  }

  public static <TValue extends Comparable<TValue>> ValueRange<TValue> of(final Interval<TValue> interval) {
    return of0(interval);
  }

  @SuppressWarnings("unchecked")
  private static <TValue extends Comparable<TValue>> ValueRange<TValue> of0() {
    return EMPTY;
  }

  private static <TValue extends Comparable<TValue>> ValueRange<TValue> of0(final Interval<TValue> interval) {
    return new ValueRange<TValue>() {
      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public boolean isFull() {
        return interval.isFull();
      }

      @Override
      public boolean isPoint() {
        return interval.isPoint();
      }

      @Override
      public boolean isInfinite() {
        return interval.isInfinite();
      }

      @Override
      public int getIntervalCount() {
        return 1;
      }

      @Override
      public Interval<TValue> getInterval(final int index) {
        if (index != 0) {
          throw new IndexOutOfBoundsException();
        }
        return interval;
      }

      @Override
      public boolean contains(final TValue value) {
        return interval.contains(value);
      }

      @Override
      public ContainsResult contains(final Interval<TValue> anotherInterval) {
        return interval.contains(anotherInterval);
      }
    };
  }

  private static <TValue extends Comparable<TValue>> ValueRange<TValue> of0(
      final Interval<TValue> interval1,
      final Interval<TValue> interval2
  ) {
    return new ValueRange<TValue>() {
      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public boolean isFull() {
        return false;
      }

      @Override
      public boolean isPoint() {
        return false;
      }

      @Override
      public boolean isInfinite() {
        return interval1.getMinimum() == null || interval2.getMaximum() == null;
      }

      @Override
      public int getIntervalCount() {
        return 2;
      }

      @Override
      public Interval<TValue> getInterval(final int index) {
        switch (index) {
          case 0:
            return interval1;
          case 1:
            return interval2;
          default:
            throw new IndexOutOfBoundsException();
        }
      }

      @Override
      public boolean contains(final TValue value) {
        return interval1.contains(value) || interval2.contains(value);
      }

      @Override
      public ContainsResult contains(final Interval<TValue> interval) {
        final ContainsResult result = interval1.contains(interval);
        switch (result) {
          case COMPLETELY:
            return ContainsResult.COMPLETELY;
          case PARTIALLY:
            return ContainsResult.PARTIALLY;
          case NOT_CONTAINS:
            break;
          default:
            throw new UnsupportedOperationException(result.name());
        }
        return interval2.contains(interval);
      }
    };
  }

  private static <TValue extends Comparable<TValue>> ValueRange<TValue> of0(final List<Interval<TValue>> intervalList) {
    if (intervalList.size() == 1) {
      return of0(intervalList.get(0));
    }
    return new ValueRange<TValue>() {
      @Override
      public boolean isEmpty() {
        return false;
      }

      @Override
      public boolean isFull() {
        return false;
      }

      @Override
      public boolean isPoint() {
        return false;
      }

      @Override
      public boolean isInfinite() {
        return intervalList.get(0).getMinimum() == null || CollectionHelper.getLast(intervalList).getMaximum() == null;
      }

      @Override
      public int getIntervalCount() {
        return intervalList.size();
      }

      @Override
      public Interval<TValue> getInterval(int index) {
        return intervalList.get(index);
      }

      @Override
      public boolean contains(final TValue value) {
        for (int index = 0; index < intervalList.size(); index++) {
          if (intervalList.get(index).contains(value)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public ContainsResult contains(final Interval<TValue> interval) {
        for (int index = 0; index < intervalList.size(); index++) {
          final ContainsResult result = intervalList.get(index).contains(interval);
          switch (result) {
            case COMPLETELY:
              return ContainsResult.COMPLETELY;
            case PARTIALLY:
              return ContainsResult.PARTIALLY;
            case NOT_CONTAINS:
              break;
            default:
              throw new UnsupportedOperationException(result.name());
          }
        }
        return ContainsResult.NOT_CONTAINS;
      }
    };
  }

  public abstract boolean isEmpty();

  public abstract boolean isFull();

  public abstract boolean isPoint();

  public abstract boolean isInfinite();

  public abstract int getIntervalCount();

  public abstract Interval<TValue> getInterval(int index);

  public abstract boolean contains(TValue value);

  public abstract ContainsResult contains(Interval<TValue> interval);

  public ValueRange<TValue> and(final Interval<TValue> interval) {
    if (interval == null) {
      throw new IllegalArgumentException("Argument [interval] is null.");
    }
    if (this.getIntervalCount() == 1) {
      return of0(and(this.getInterval(0), interval));
    }
    if (this.isEmpty()) {
      return of0();
    }
    if (interval.isFull()) {
      return this;
    }
    final List<Interval<TValue>> result = new ArrayList<>();
    Interval<TValue> intervalInRange = this.getInterval(0);
    int thisIntervalIndex = 1;
    while (true) {
      final Interval<TValue> andInterval = and(interval, intervalInRange);
      if (andInterval != null) {
        result.add(andInterval);
      }
      if (thisIntervalIndex == this.getIntervalCount()) {
        break;
      }
      intervalInRange = this.getInterval(thisIntervalIndex);
      thisIntervalIndex++;
      if (interval.getMaximum() != null) {
        final int compareResult = intervalInRange.getMinimum().compareTo(interval.getMaximum());
        if (compareResult > 0) {
          break;
        }
        if (compareResult == 0) {
          if (intervalInRange.isIncludeMinimum() && interval.isIncludeMaximum()) {
            result.add(Interval.of(intervalInRange.getMinimum(), true, intervalInRange.getMinimum(), true));
          }
          break;
        }
      }
    }
    if (result.isEmpty()) {
      return of0();
    } else {
      return ValueRange.of0(result);
    }
  }

  public ValueRange<TValue> and(final ValueRange<TValue> that) {
    if (that == null) {
      throw new IllegalArgumentException("Argument [that] is null.");
    }
    if (this.getIntervalCount() == 1) {
      if (that.getIntervalCount() == 1) {
        final Interval<TValue> interval = and(this.getInterval(0), that.getInterval(0));
        return interval == null ? ValueRange.of0() : of0(interval);
      } else {
        return that.and(this.getInterval(0));
      }
    } else {
      if (that.getIntervalCount() == 1) {
        return this.and(that.getInterval(0));
      }
    }
    if (this.isEmpty() || that.isEmpty()) {
      return of0();
    }
    final ArrayList<Interval<TValue>> result = new ArrayList<>();
    Interval<TValue> thisInterval = this.getInterval(0);
    Interval<TValue> thatInterval = that.getInterval(0);
    int thisIntervalIndex = 1;
    int thatIntervalIndex = 1;
    while (true) {
      final Interval<TValue> andInterval = and(thisInterval, thatInterval);
      if (andInterval != null) {
        result.add(andInterval);
      }
      if (thisInterval.getMaximum() == null) {
        if (thatInterval.getMaximum() == null) {
          break;
        } else {
          if (thatIntervalIndex == that.getIntervalCount()) {
            break;
          } else {
            thatInterval = that.getInterval(thatIntervalIndex);
            thatIntervalIndex++;
            continue;
          }
        }
      } else {
        if (thatInterval.getMaximum() == null) {
          if (thisIntervalIndex == this.getIntervalCount()) {
            break;
          } else {
            thisInterval = this.getInterval(thisIntervalIndex);
            thisIntervalIndex++;
            continue;
          }
        }
      }
      if (thisIntervalIndex == this.getIntervalCount() || thatIntervalIndex == that.getIntervalCount()) {
        break;
      }
      final int compareResult = thisInterval.getMaximum().compareTo(thatInterval.getMaximum());
      if (compareResult < 0) {
        thisInterval = this.getInterval(thisIntervalIndex);
        thisIntervalIndex++;
      } else if (compareResult > 0) {
        thatInterval = that.getInterval(thatIntervalIndex);
        thatIntervalIndex++;
      } else {
        if (thisInterval.isIncludeMaximum()) {
          if (!thatInterval.isIncludeMaximum()) {
            thatInterval = that.getInterval(thatIntervalIndex);
            thatIntervalIndex++;
            continue;
          }
        } else {
          if (thatInterval.isIncludeMaximum()) {
            thisInterval = this.getInterval(thisIntervalIndex);
            thisIntervalIndex++;
            continue;
          }
        }
        thisInterval = this.getInterval(thisIntervalIndex);
        thisIntervalIndex++;
        thatInterval = that.getInterval(thatIntervalIndex);
        thatIntervalIndex++;
      }
    }
    if (result.isEmpty()) {
      return of0();
    } else {
      return ValueRange.of0(result);
    }
  }

  private static <TValue extends Comparable<TValue>> Interval<TValue> and(
      final Interval<TValue> interval1,
      final Interval<TValue> interval2
  ) {
    final TValue minimum;
    final boolean includeMinimum;
    if (interval1.getMinimum() == null) {
      minimum = interval2.getMinimum();
      includeMinimum = interval2.isIncludeMinimum();
    } else {
      if (interval2.getMinimum() == null) {
        minimum = interval1.getMinimum();
        includeMinimum = interval1.isIncludeMinimum();
      } else {
        final int compareResult = interval1.getMinimum().compareTo(interval2.getMinimum());
        if (compareResult < 0) {
          minimum = interval2.getMinimum();
          includeMinimum = interval2.isIncludeMinimum();
        } else if (compareResult == 0) {
          minimum = interval1.getMinimum();
          includeMinimum = interval1.isIncludeMinimum() & interval2.isIncludeMinimum();
        } else {
          minimum = interval1.getMinimum();
          includeMinimum = interval1.isIncludeMinimum();
        }
      }
    }
    final TValue maximum;
    final boolean includeMaximum;
    if (interval1.getMaximum() == null) {
      maximum = interval2.getMaximum();
      includeMaximum = interval2.isIncludeMaximum();
    } else {
      if (interval2.getMaximum() == null) {
        maximum = interval1.getMaximum();
        includeMaximum = interval1.isIncludeMaximum();
      } else {
        final int compareResult = interval1.getMaximum().compareTo(interval2.getMaximum());
        if (compareResult < 0) {
          maximum = interval1.getMaximum();
          includeMaximum = interval1.isIncludeMaximum();
        } else if (compareResult == 0) {
          maximum = interval1.getMaximum();
          includeMaximum = interval1.isIncludeMaximum() & interval2.isIncludeMaximum();
        } else {
          maximum = interval2.getMaximum();
          includeMaximum = interval2.isIncludeMaximum();
        }
      }
    }
    if (minimum != null && maximum != null) {
      final int compareResult = minimum.compareTo(maximum);
      if (compareResult > 0 || (compareResult == 0 && (!includeMinimum || !includeMaximum))) {
        return null;
      }
    }
    if (interval1.equals(minimum, includeMinimum, maximum, includeMaximum)) {
      return interval1;
    }
    if (interval2.equals(minimum, includeMinimum, maximum, includeMaximum)) {
      return interval2;
    }
    return Interval.of(minimum, includeMinimum, maximum, includeMaximum);
  }

  public ValueRange<TValue> or(Interval<TValue> interval) {
    if (interval == null) {
      throw new IllegalArgumentException("Argument [interval] is null.");
    }

    if (this.getIntervalCount() == 1) {
      return or(this.getInterval(0), interval);
    }

    if (this.isEmpty()) {
      return ValueRange.of0(interval);
    }

    if (interval.isFull()) {
      return full();
    }
    final ArrayList<Interval<TValue>> result = new ArrayList<>();
    Interval<TValue> thisInterval = this.getInterval(0);
    int thisIntervalIndex = 1;
    if (interval.getMinimum() == null) {
      // interval.getMaximum() != null
      while (true) {
        if (thisInterval.getMaximum() == null) {
          final ValueRange<TValue> orInterval = or(thisInterval, interval);
          result.add(orInterval.getInterval(0));
          if (orInterval.getIntervalCount() == 2) {
            result.add(orInterval.getInterval(1));
          }
          break;
        } else {
          final int compareResult = thisInterval.getMaximum().compareTo(interval.getMaximum());
          if (compareResult > 0) {
            final ValueRange<TValue> orInterval = or(thisInterval, interval);
            result.add(orInterval.getInterval(0));
            if (orInterval.getIntervalCount() == 2) {
              result.add(orInterval.getInterval(1));
            }
            for (; thisIntervalIndex < this.getIntervalCount(); thisIntervalIndex++) {
              result.add(this.getInterval(thisIntervalIndex));
            }
            break;
          }
          if (compareResult == 0 && !interval.isIncludeMaximum()) {
            if (thisInterval.isIncludeMaximum()) {
              interval = Interval.of(null, false, interval.getMaximum(), true);
            } else {
              result.add(interval);
              for (; thisIntervalIndex < this.getIntervalCount(); thisIntervalIndex++) {
                result.add(this.getInterval(thisIntervalIndex));
              }
              break;
            }
          }
          thisInterval = this.getInterval(thisIntervalIndex);
          thisIntervalIndex++;
        }
      }

    } else if (interval.getMaximum() == null) {
      // interval.getMinimum() != null
      while (true) {
        if (thisInterval.getMaximum() == null) {
          final ValueRange<TValue> orInterval = or(thisInterval, interval);
          result.add(orInterval.getInterval(0));
          if (orInterval.getIntervalCount() == 2) {
            result.add(orInterval.getInterval(1));
          }
          break;
        } else {
          final int compareResult = thisInterval.getMaximum().compareTo(interval.getMinimum());
          if (compareResult > 0) {
            final ValueRange<TValue> orInterval = or(thisInterval, interval);
            result.add(orInterval.getInterval(0));
            if (orInterval.getIntervalCount() == 2) {
              result.add(orInterval.getInterval(1));
            }
            break;
          }
          if (compareResult == 0) {
            if (thisInterval.isIncludeMaximum() | interval.isIncludeMinimum()) {
              result.add(Interval.of(thisInterval.getMinimum(), thisInterval.isIncludeMinimum(), null, false));
            } else {
              result.add(thisInterval);
              result.add(interval);
            }
            break;
          }
          result.add(thisInterval);
          thisInterval = this.getInterval(thisIntervalIndex);
          thisIntervalIndex++;
        }
      }
    } else {
      // interval.getMinimum() != null
      // interval.getMaximum() != null
      while (true) {
        if (thisIntervalIndex == this.getIntervalCount()) {
          // The last interval.
          final ValueRange<TValue> orInterval = or(thisInterval, interval);
          result.add(orInterval.getInterval(0));
          if (orInterval.getIntervalCount() == 2) {
            result.add(orInterval.getInterval(1));
          }
          break;
        }
        int compareResult = thisInterval.getMaximum().compareTo(interval.getMinimum());
        if (compareResult < 0) {
          result.add(thisInterval);
          thisInterval = this.getInterval(thisIntervalIndex);
          thisIntervalIndex++;
          continue;
        }
        if (compareResult == 0) {
          if (thisInterval.isIncludeMaximum() | interval.isIncludeMinimum()) {
            interval = Interval.of(
                thisInterval.getMinimum(),
                thisInterval.isIncludeMinimum(),
                interval.getMaximum(),
                interval.isIncludeMaximum());
          } else {
            result.add(thisInterval);
          }
          thisInterval = this.getInterval(thisIntervalIndex);
          thisIntervalIndex++;
          continue;
        }
        if (thisInterval.getMinimum() != null) {
          compareResult = thisInterval.getMinimum().compareTo(interval.getMaximum());
          if (compareResult > 0) {
            result.add(interval);
            result.add(thisInterval);
            for (; thisIntervalIndex < this.getIntervalCount(); thisIntervalIndex++) {
              result.add(this.getInterval(thisIntervalIndex));
            }
            break;
          }
          if (compareResult == 0) {
            if (thisInterval.isIncludeMinimum() | interval.isIncludeMaximum()) {
              result.add(Interval.of(
                  interval.getMinimum(),
                  interval.isIncludeMinimum(),
                  thisInterval.getMaximum(),
                  thisInterval.isIncludeMaximum()));
            } else {
              result.add(interval);
              result.add(thisInterval);
            }
            for (; thisIntervalIndex < this.getIntervalCount(); thisIntervalIndex++) {
              result.add(this.getInterval(thisIntervalIndex));
            }
            break;
          }
        }
        interval = or(thisInterval, interval).getInterval(0);
        thisInterval = this.getInterval(thisIntervalIndex);
        thisIntervalIndex++;
      }
    }

    if (result.isEmpty()) {
      return of0();
    } else {
      if (result.get(0).isFull()) {
        return full();
      } else {
        return ValueRange.of0(result);
      }
    }
  }

  public ValueRange<TValue> or(final ValueRange<TValue> that) {
    if (that == null) {
      throw new IllegalArgumentException("Argument [that] is null.");
    }
    if (this.getIntervalCount() == 1) {
      if (that.getIntervalCount() == 1) {
        return or(this.getInterval(0), that.getInterval(0));
      } else {
        return that.or(this.getInterval(0));
      }
    } else {
      if (that.getIntervalCount() == 1) {
        return this.or(that.getInterval(0));
      }
    }
    if (this.isEmpty()) {
      if (that.isEmpty()) {
        return of0();
      } else {
        return that;
      }
    }
    if (that.isEmpty()) {
      return this;
    }
    final ArrayList<Interval<TValue>> result = new ArrayList<>();
    Interval<TValue> interval = null;
    Interval<TValue> thisInterval = null;
    Interval<TValue> thatInterval = null;
    int thisIntervalIndex = 0;
    int thatIntervalIndex = 0;
    while (true) {
      if (thisInterval == null) {
        if (thisIntervalIndex == this.getIntervalCount()) {
          break;
        }
        thisInterval = this.getInterval(thisIntervalIndex);
        thisIntervalIndex++;
        if (thatInterval == null) {
          if (thatIntervalIndex == that.getIntervalCount()) {
            break;
          }
          thatInterval = that.getInterval(thatIntervalIndex);
          thatIntervalIndex++;
          interval = getLess(thisInterval, thatInterval);
          if (interval == thisInterval) {
            thisInterval = null;
          } else {
            thatInterval = null;
          }
          continue;
        }
      } else {
        // thatInterval == null;
        if (thatIntervalIndex == that.getIntervalCount()) {
          break;
        }
        thatInterval = that.getInterval(thatIntervalIndex);
        thatIntervalIndex++;
      }
      final Interval<TValue> lessInterval = getLess(thisInterval, thatInterval);
      final ValueRange<TValue> orInterval = or(interval, lessInterval);
      switch (orInterval.getIntervalCount()) {
        case 1:
          interval = orInterval.getInterval(0);
          break;
        case 2:
          result.add(orInterval.getInterval(0));
          interval = orInterval.getInterval(1);
          break;
        default:
          throw new RuntimeException();
      }
      if (lessInterval == thisInterval) {
        thisInterval = null;
      } else {
        thatInterval = null;
      }
    }
    if (thisInterval != null) {
      @SuppressWarnings("ConstantConditions") final ValueRange<TValue> orInterval = or(interval, thisInterval);
      switch (orInterval.getIntervalCount()) {
        case 1:
          interval = orInterval.getInterval(0);
          break;
        case 2:
          result.add(orInterval.getInterval(0));
          interval = orInterval.getInterval(1);
          break;
        default:
          throw new RuntimeException();
      }
    }
    if (thatInterval != null) {
      final ValueRange<TValue> orInterval = or(interval, thatInterval);
      switch (orInterval.getIntervalCount()) {
        case 1:
          interval = orInterval.getInterval(0);
          break;
        case 2:
          result.add(orInterval.getInterval(0));
          interval = orInterval.getInterval(1);
          break;
        default:
          throw new RuntimeException();
      }
    }
    result.add(interval);
    for (; thisIntervalIndex < this.getIntervalCount(); thisIntervalIndex++) {
      result.add(this.getInterval(thisIntervalIndex));
    }
    for (; thatIntervalIndex < that.getIntervalCount(); thatIntervalIndex++) {
      result.add(that.getInterval(thatIntervalIndex));
    }
    if (result.isEmpty()) {
      return of0();
    } else {
      if (result.get(0).isFull()) {
        return full();
      } else {
        return ValueRange.of0(result);
      }
    }
  }

  private static <TValue extends Comparable<TValue>> ValueRange<TValue> or(
      final Interval<TValue> interval1,
      final Interval<TValue> interval2
  ) {
    if (interval2 == null) {
      throw new IllegalArgumentException("Argument [that] is null.");
    }
    final Interval<TValue> lessInterval = getLess(interval1, interval2);
    final Interval<TValue> greaterInterval = lessInterval == interval1 ? interval2 : interval1;
    final boolean haveIntersection;
    if (lessInterval.getMaximum() == null) {
      haveIntersection = true;
    } else {
      if (greaterInterval.getMinimum() == null) {
        haveIntersection = true;
      } else {
        final int compareResult = lessInterval.getMaximum().compareTo(greaterInterval.getMinimum());
        if (compareResult < 0) {
          haveIntersection = false;
        } else if (compareResult == 0) {
          if (lessInterval.isIncludeMaximum() | greaterInterval.isIncludeMinimum()) {
            return ValueRange.of0(Interval.of(
                lessInterval.getMinimum(),
                lessInterval.isIncludeMinimum(),
                greaterInterval.getMaximum(),
                greaterInterval.isIncludeMaximum()));
          }
          haveIntersection = false;
        } else {
          haveIntersection = true;
        }
      }
    }
    if (haveIntersection) {
      final TValue maximum;
      final boolean includeMaximum;
      if (lessInterval.getMaximum() == null || greaterInterval.getMaximum() == null) {
        maximum = null;
        includeMaximum = false;
      } else {
        final int compareResult = lessInterval.getMaximum().compareTo(greaterInterval.getMaximum());
        if (compareResult < 0) {
          maximum = greaterInterval.getMaximum();
          includeMaximum = greaterInterval.isIncludeMaximum();
        } else if (compareResult == 0) {
          maximum = lessInterval.getMaximum();
          includeMaximum = lessInterval.isIncludeMaximum() | greaterInterval.isIncludeMaximum();
        } else {
          maximum = lessInterval.getMaximum();
          includeMaximum = lessInterval.isIncludeMaximum();
        }
      }
      if (interval1.equals(lessInterval.getMinimum(), lessInterval.isIncludeMinimum(), maximum, includeMaximum)) {
        return ValueRange.of0(interval1);
      }
      if (interval2.equals(lessInterval.getMinimum(), lessInterval.isIncludeMinimum(), maximum, includeMaximum)) {
        return ValueRange.of0(interval2);
      }
      return ValueRange.of0(Interval.of(lessInterval.getMinimum(), lessInterval.isIncludeMinimum(), maximum, includeMaximum));
    } else {
      return ValueRange.of0(lessInterval, greaterInterval);
    }
  }

  public ValueRange<TValue> not() {
    if (this.getIntervalCount() == 1) {
      return not(this.getInterval(0));
    }
    if (this.isEmpty()) {
      return full();
    }
    final List<Interval<TValue>> result = new ArrayList<>(this.getIntervalCount() + 1);
    final Interval<TValue> firstInterval = this.getInterval(0);
    if (firstInterval.getMinimum() != null) {
      result.add(Interval.of(null, false, firstInterval.getMinimum(), !firstInterval.isIncludeMinimum()));
    }
    TValue lastIntervalMaximum = firstInterval.getMaximum();
    boolean lastIntervalIncludeMaximum = firstInterval.isIncludeMaximum();
    for (int i = 1; i < this.getIntervalCount(); i++) {
      final Interval<TValue> interval = this.getInterval(i);
      result.add(Interval.of(lastIntervalMaximum, !lastIntervalIncludeMaximum, interval.getMinimum(), !interval.isIncludeMinimum()));
      lastIntervalMaximum = interval.getMaximum();
      lastIntervalIncludeMaximum = interval.isIncludeMaximum();
    }
    if (lastIntervalMaximum != null) {
      result.add(Interval.of(lastIntervalMaximum, !lastIntervalIncludeMaximum, null, false));
    }
    return ValueRange.of0(result);
  }

  private static <TValue extends Comparable<TValue>> ValueRange<TValue> not(final Interval<TValue> interval) {
    if (interval.isFull()) {
      return ValueRange.of0();
    }
    if (interval.getMinimum() == null) {
      return ValueRange.of0(Interval.of(interval.getMaximum(), !interval.isIncludeMaximum(), null, false));
    }
    if (interval.getMaximum() == null) {
      return ValueRange.of0(Interval.of(null, false, interval.getMinimum(), !interval.isIncludeMinimum()));
    }
    return ValueRange.of0(
        Interval.of(null, false, interval.getMinimum(), !interval.isIncludeMinimum()),
        Interval.of(interval.getMaximum(), !interval.isIncludeMaximum(), null, false)
    );
  }

  private static <TValue extends Comparable<TValue>> Interval<TValue> getLess(
      final Interval<TValue> interval1,
      final Interval<TValue> interval2) {
    if (interval1.getMinimum() == null) {
      return interval1;
    } else {
      if (interval2.getMinimum() == null) {
        return interval2;
      } else {
        final int compareResult = interval1.getMinimum().compareTo(interval2.getMinimum());
        if (compareResult < 0) {
          return interval1;
        } else if (compareResult == 0) {
          if (interval1.isIncludeMinimum()) {
            return interval1;
          } else {
            return interval2;
          }
        } else {
          return interval2;
        }
      }
    }
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    for (int index = 0; index < this.getIntervalCount(); index++) {
      hashCode = 31 * hashCode + this.getInterval(index).hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object != null && object.getClass() == this.getClass()) {
      final ValueRange that = (ValueRange) object;
      if (this.getIntervalCount() != that.getIntervalCount()) {
        return false;
      }
      for (int index = 0; index < this.getIntervalCount(); index++) {
        if (!this.getInterval(index).equals(that.getInterval(index))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (int index = 0; index < this.getIntervalCount(); index++) {
      stringBuilder.append(this.getInterval(index).toString());
    }
    return stringBuilder.toString();
  }

  public static abstract class Interval<TValue extends Comparable<TValue>> {

    @SuppressWarnings("unchecked")
    private static final Interval FULL = new Interval() {
      @Override
      public Comparable getMinimum() {
        return null;
      }

      @Override
      public Comparable getMaximum() {
        return null;
      }

      @Override
      public boolean isIncludeMinimum() {
        return false;
      }

      @Override
      public boolean isIncludeMaximum() {
        return false;
      }

      @Override
      public boolean isFull() {
        return true;
      }

      @Override
      public boolean isPoint() {
        return false;
      }

      @Override
      public boolean isInfinite() {
        return true;
      }

      @Override
      public boolean contains(final Comparable comparable) {
        return true;
      }

      @Override
      public ContainsResult contains(final Interval that) {
        return ContainsResult.COMPLETELY;
      }
    };

    @SuppressWarnings("unchecked")
    public static <TValue extends Comparable<TValue>> Interval<TValue> full() {
      return FULL;
    }

    public static <TValue extends Comparable<TValue>> Interval<TValue> of(final TValue value) {
      if (value == null) {
        throw new IllegalArgumentException("Argument [value] is null.");
      }
      return new Interval<TValue>() {
        @Override
        public TValue getMinimum() {
          return value;
        }

        @Override
        public TValue getMaximum() {
          return value;
        }

        @Override
        public boolean isIncludeMinimum() {
          return true;
        }

        @Override
        public boolean isIncludeMaximum() {
          return true;
        }

        @Override
        public boolean isFull() {
          return false;
        }

        @Override
        public boolean isPoint() {
          return true;
        }

        @Override
        public boolean isInfinite() {
          return false;
        }

        @Override
        public boolean contains(final TValue thatValue) {
          return value.equals(thatValue);
        }

        @Override
        public ContainsResult contains(final Interval<TValue> that) {
          if (that.contains(value)) {
            if (that.isPoint() && that.getMinimum().equals(value)) {
              return ContainsResult.COMPLETELY;
            } else {
              return ContainsResult.PARTIALLY;
            }
          } else {
            return ContainsResult.NOT_CONTAINS;
          }
        }
      };
    }

    public static <TValue extends Comparable<TValue>> Interval<TValue> of(
        final TValue minimum,
        final boolean includeMinimum,
        final TValue maximum,
        final boolean includeMaximum) {
      if (minimum == null) {
        if (maximum == null) {
          return full();
        }
      } else {
        if (maximum != null) {
          final int compareResult = minimum.compareTo(maximum);
          if (compareResult > 0) {
            throw new IllegalArgumentException("Invalid interval.");
          }
          if (compareResult == 0) {
            if (!(includeMinimum && includeMaximum)) {
              throw new IllegalArgumentException("Invalid interval.");
            } else {
              return of(minimum);
            }
          }
        }
      }
      final boolean includeMinimum0 = minimum != null && includeMinimum;
      final boolean includeMaximum0 = maximum != null && includeMaximum;
      return new Interval<TValue>() {
        @Override
        public TValue getMinimum() {
          return minimum;
        }

        @Override
        public TValue getMaximum() {
          return maximum;
        }

        @Override
        public boolean isIncludeMinimum() {
          return includeMinimum0;
        }

        @Override
        public boolean isIncludeMaximum() {
          return includeMaximum0;
        }

        @Override
        public boolean isFull() {
          return false;
        }

        @Override
        public boolean isPoint() {
          return false;
        }

        @Override
        public boolean isInfinite() {
          return minimum == null || maximum == null;
        }

        @Override
        public boolean contains(final TValue value) {
          if (maximum != null) {
            final int compareResult = value.compareTo(maximum);
            if (compareResult > 0) {
              return false;
            }
            if (compareResult == 0 && !includeMaximum) {
              return false;
            }
          }
          if (minimum != null) {
            final int compareResult = minimum.compareTo(value);
            if (compareResult > 0) {
              return false;
            } else {
              return compareResult != 0 || includeMinimum;
            }
          } else {
            return true;
          }
        }

        @Override
        public ContainsResult contains(final Interval<TValue> that) {
          if (that.isFull()) {
            return ContainsResult.PARTIALLY;
          }
          if (that.isPoint()) {
            return this.contains(that.getMinimum()) ? ContainsResult.COMPLETELY : ContainsResult.NOT_CONTAINS;
          }
          if (this.getMinimum() == null) {
            if (that.getMinimum() == null) {
              final int compareResult = this.getMaximum().compareTo(that.getMaximum());
              if (compareResult > 0) {
                return ContainsResult.COMPLETELY;
              }
              if (compareResult < 0) {
                return ContainsResult.PARTIALLY;
              }
              if (this.isIncludeMaximum()) {
                return ContainsResult.COMPLETELY;
              }
              return that.isIncludeMaximum() ? ContainsResult.PARTIALLY : ContainsResult.COMPLETELY;
            }
            if (that.getMaximum() == null) {
              final int compareResult = this.getMaximum().compareTo(that.getMinimum());
              if (compareResult < 0) {
                return ContainsResult.NOT_CONTAINS;
              }
              if (compareResult > 0) {
                return ContainsResult.PARTIALLY;
              }
              if (this.isIncludeMaximum() & that.isIncludeMinimum()) {
                return ContainsResult.PARTIALLY;
              } else {
                return ContainsResult.NOT_CONTAINS;
              }
            }
            int compareResult = this.getMaximum().compareTo(that.getMinimum());
            if (compareResult < 0) {
              return ContainsResult.NOT_CONTAINS;
            }
            if (compareResult == 0) {
              if (this.isIncludeMaximum() & that.isIncludeMinimum()) {
                return that.isPoint() ? ContainsResult.COMPLETELY : ContainsResult.PARTIALLY;
              }
              return ContainsResult.NOT_CONTAINS;
            }
            compareResult = this.getMaximum().compareTo(that.getMaximum());
            if (compareResult > 0) {
              return ContainsResult.COMPLETELY;
            }
            if (compareResult < 0) {
              return ContainsResult.PARTIALLY;
            }
            if (this.isIncludeMaximum() | !that.isIncludeMaximum()) {
              return ContainsResult.COMPLETELY;
            } else {
              return ContainsResult.PARTIALLY;
            }
          }
          if (this.getMaximum() == null) {
            if (that.getMinimum() == null) {
              final int compareResult = this.getMinimum().compareTo(that.getMaximum());
              if (compareResult < 0) {
                return ContainsResult.PARTIALLY;
              }
              if (compareResult > 0) {
                return ContainsResult.NOT_CONTAINS;
              }
              if (this.isIncludeMinimum() & that.isIncludeMaximum()) {
                return ContainsResult.PARTIALLY;
              } else {
                return ContainsResult.NOT_CONTAINS;
              }
            }
            if (that.getMaximum() == null) {
              final int compareResult = this.getMinimum().compareTo(that.getMinimum());
              if (compareResult < 0) {
                return ContainsResult.COMPLETELY;
              }
              if (compareResult > 0) {
                return ContainsResult.PARTIALLY;
              }
              if (this.isIncludeMinimum()) {
                return ContainsResult.COMPLETELY;
              }
              return that.isIncludeMinimum() ? ContainsResult.PARTIALLY : ContainsResult.COMPLETELY;
            }
            int compareResult = this.getMinimum().compareTo(that.getMaximum());
            if (compareResult > 0) {
              return ContainsResult.NOT_CONTAINS;
            }
            if (compareResult == 0) {
              if (this.isIncludeMinimum() & that.isIncludeMaximum()) {
                return that.isPoint() ? ContainsResult.COMPLETELY : ContainsResult.PARTIALLY;
              } else {
                return ContainsResult.NOT_CONTAINS;
              }
            }
            compareResult = this.getMinimum().compareTo(that.getMinimum());
            if (compareResult < 0) {
              return ContainsResult.COMPLETELY;
            }
            if (compareResult > 0) {
              return ContainsResult.PARTIALLY;
            }
            if (this.isIncludeMinimum() | !that.isIncludeMinimum()) {
              return ContainsResult.COMPLETELY;
            } else {
              return ContainsResult.PARTIALLY;
            }
          }
          if (that.getMinimum() == null) {
            final int compareResult = this.getMinimum().compareTo(that.getMaximum());
            if (compareResult < 0) {
              return ContainsResult.PARTIALLY;
            }
            if (compareResult > 0) {
              return ContainsResult.NOT_CONTAINS;
            }
            if (this.isIncludeMinimum() & that.isIncludeMaximum()) {
              return ContainsResult.PARTIALLY;
            } else {
              return ContainsResult.NOT_CONTAINS;
            }
          }
          if (that.getMaximum() == null) {
            final int compareResult = this.getMaximum().compareTo(that.getMinimum());
            if (compareResult < 0) {
              return ContainsResult.NOT_CONTAINS;
            }
            if (compareResult > 0) {
              return ContainsResult.PARTIALLY;
            }
            if (this.isIncludeMinimum() & that.isIncludeMaximum()) {
              return ContainsResult.PARTIALLY;
            } else {
              return ContainsResult.NOT_CONTAINS;
            }
          }
          int compareResult = this.getMaximum().compareTo(that.getMinimum());
          if (compareResult < 0) {
            return ContainsResult.NOT_CONTAINS;
          }
          if (compareResult == 0) {
            if (this.isIncludeMaximum() & that.isIncludeMinimum()) {
              return that.isPoint() ? ContainsResult.COMPLETELY : ContainsResult.PARTIALLY;
            } else {
              return ContainsResult.NOT_CONTAINS;
            }
          }
          compareResult = this.getMinimum().compareTo(that.getMaximum());
          if (compareResult > 0) {
            return ContainsResult.NOT_CONTAINS;
          }
          if (compareResult == 0) {
            if (this.isIncludeMinimum() & that.isIncludeMaximum()) {
              return ContainsResult.PARTIALLY;
            } else {
              return ContainsResult.NOT_CONTAINS;
            }
          }
          compareResult = this.getMinimum().compareTo(that.getMinimum());
          if (compareResult > 0) {
            return ContainsResult.PARTIALLY;
          }
          if (compareResult == 0) {
            if (!this.isIncludeMinimum() && that.isIncludeMinimum()) {
              return ContainsResult.PARTIALLY;
            }
          }
          compareResult = this.getMaximum().compareTo(that.getMaximum());
          if (compareResult > 0) {
            return ContainsResult.COMPLETELY;
          }
          if (compareResult < 0) {
            return ContainsResult.PARTIALLY;
          }
          if (this.isIncludeMaximum() == that.isIncludeMaximum()) {
            return ContainsResult.COMPLETELY;
          } else {
            return ContainsResult.PARTIALLY;
          }
        }
      };
    }

    public abstract TValue getMinimum();

    public abstract TValue getMaximum();

    public abstract boolean isIncludeMinimum();

    public abstract boolean isIncludeMaximum();

    public abstract boolean isFull();

    public abstract boolean isPoint();

    public abstract boolean isInfinite();

    public abstract boolean contains(TValue value);

    public abstract ContainsResult contains(Interval<TValue> that);

    @Override
    public String toString() {
      return ""
          + (this.isIncludeMinimum() ? "[" : "(")
          + (this.getMinimum() == null ? "" : this.getMinimum().toString())
          + ","
          + (this.getMaximum() == null ? "" : this.getMaximum().toString())
          + (this.isIncludeMaximum() ? "]" : ")");
    }

    @Override
    public int hashCode() {
      final int hashCode = this.getMinimum() == null ? 0 : this.getMinimum().hashCode();
      return 31 * hashCode + (this.getMaximum() == null ? 0 : this.getMaximum().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object != null && object.getClass() == this.getClass()) {
        final Interval that = (Interval) object;
        return this.equals(that.getMinimum(), that.isIncludeMinimum(), that.getMaximum(), that.isIncludeMaximum());
      } else {
        return false;
      }
    }

    private boolean equals(final Object minimum, final boolean includeMinimum, final Object maximum, final boolean includeMaximum) {
      final boolean isMinimumEquals;
      if (this.getMinimum() == null) {
        isMinimumEquals = minimum == null;
      } else {
        isMinimumEquals = this.getMinimum().equals(minimum) && this.isIncludeMinimum() == includeMinimum;
      }

      final boolean isMaximumEquals;
      if (this.getMaximum() == null) {
        isMaximumEquals = maximum == null;
      } else {
        isMaximumEquals = this.getMaximum().equals(maximum) && this.isIncludeMaximum() == includeMaximum;
      }

      return isMinimumEquals & isMaximumEquals;
    }

  }

  public enum IntervalBoundary {

    MINIMUM() {
      @Override
      public <TValue extends Comparable<TValue>> TValue get(final Interval<TValue> interval) {
        return interval.getMinimum();
      }

      @Override
      public boolean isInfinite(final Interval interval) {
        return interval.getMinimum() == null;
      }
    },

    MAXIMUM() {
      @Override
      public <TValue extends Comparable<TValue>> TValue get(final Interval<TValue> interval) {
        return interval.getMaximum();
      }

      @Override
      public boolean isInfinite(final Interval interval) {
        return interval.getMaximum() == null;
      }
    };

    public abstract <TValue extends Comparable<TValue>> TValue get(Interval<TValue> interval);

    public abstract boolean isInfinite(Interval interval);

  }

  public enum ContainsResult {

    COMPLETELY,

    PARTIALLY,

    NOT_CONTAINS

  }

}
