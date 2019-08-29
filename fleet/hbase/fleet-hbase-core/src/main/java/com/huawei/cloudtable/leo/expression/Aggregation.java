package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.Identifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

public abstract class Aggregation<TResult> extends Expression<TResult> implements Function<TResult> {

  public Aggregation(final Class<TResult> resultClass, final boolean nullable) {
    this(resultClass, nullable, Evaluation.EMPTY_LIST);
  }

  public Aggregation(final Class<TResult> resultClass, final boolean nullable, final Evaluation... parameterList) {
    super(resultClass, nullable);
    if (parameterList == null) {
      throw new IllegalArgumentException("Argument [parameterList] is null.");
    }
    this.name = Function.getName(this.getClass());
    this.parameterList = parameterList;
  }

  private final Identifier name;

  private final Evaluation[] parameterList;

  private volatile int hashCode = 0;

  @Nonnull
  @Override
  public Identifier getName() {
    return name;
  }

  public final int getParameterCount() {
    return this.parameterList.length;
  }

  /**
   * @throws IndexOutOfBoundsException If parameter index out build bounds.
   */
  @Nullable
  public final Evaluation<?> getParameter(final int parameterIndex) {
    return this.parameterList[parameterIndex];
  }

  public abstract Aggregator<TResult> newAggregator(Evaluation.CompileContext context);

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      if (this instanceof Commutative) {
        this.hashCode = 31 * this.name.hashCode() + ((Commutative) this).getHashCode();
      } else {
        this.hashCode = 31 * this.name.hashCode() + Arrays.hashCode(this.parameterList);
      }
    }
    return this.hashCode;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (object.getClass() == this.getClass()) {
      final Aggregation that = (Aggregation) object;
      if (that instanceof Commutative) {
        return this.name.equals(that.name) && ((Commutative) this).equals((Commutative) that);
      } else {
        return this.name.equals(that.name) && Arrays.equals(this.parameterList, that.parameterList);
      }
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    Function.toString(this, stringBuilder);
  }

}
