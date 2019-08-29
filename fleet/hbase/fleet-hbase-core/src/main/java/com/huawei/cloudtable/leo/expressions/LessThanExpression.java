package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.expression.Comparison;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;
import com.huawei.cloudtable.leo.value.*;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class LessThanExpression<TParameter extends Comparable<TParameter>>
    extends Evaluation<Boolean> implements Function<Boolean>, Comparison<TParameter> {

  public static final String NAME = "<";

  public LessThanExpression(final Evaluation<TParameter> parameter1, final Evaluation<TParameter> parameter2) {
    super(Boolean.class, false, parameter1, parameter2);
    if (parameter1 == null || parameter2 == null) {
      // TODO
      throw new UnsupportedOperationException();
    }
    this.name = Function.getName(this.getClass());
  }

  private final Identifier name;

  private Comparator comparator;

  @Nonnull
  @Override
  public Identifier getName() {
    return this.name;
  }

  @Nonnull
  @Override
  @SuppressWarnings("ConstantConditions")
  public Evaluation<TParameter> getParameter1() {
    return super.getParameter(0);
  }

  @Nonnull
  @Override
  @SuppressWarnings("ConstantConditions")
  public Evaluation<TParameter> getParameter2() {
    return super.getParameter(1);
  }

  @Override
  public ValueRange<TParameter> getValueRange(final int variableIndex) {
    return this.getValueRange(variableIndex, null);
  }

  @Override
  public ValueRange<TParameter> getValueRange(final int variableIndex, final RuntimeContext context) {
    try {
      switch (variableIndex) {
        case 0: {
          final TParameter constant = this.getParameter2().evaluate(context);
          if (constant != null) {
            return ValueRange.of(null, false, constant, false);
          }
          break;
        }
        case 1: {
          final TParameter constant = this.getParameter1().evaluate(context);
          if (constant != null) {
            return ValueRange.of(constant, true, null, false);
          }
          break;
        }
        default:
          throw new IndexOutOfBoundsException();
      }
    } catch (EvaluateException ignore) {
      // to do nothing.
    }
    return null;
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
    this.comparator = Comparison.newComparator(this, context);
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    return this.comparator.compare(context) < 0;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.getParameter1().toString(stringBuilder);
    stringBuilder.append(" ");
    stringBuilder.append(NAME);
    stringBuilder.append(" ");
    this.getParameter2().toString(stringBuilder);
  }

  @Name(NAME)
  public static final class ForBoolean extends LessThanExpression<Boolean> {

    public ForBoolean(
        @ValueParameter(clazz = Boolean.class) final Evaluation<Boolean> parameter1,
        @ValueParameter(clazz = Boolean.class) final Evaluation<Boolean> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Boolean> getParameterClass() {
      return Boolean.class;
    }

  }

  @Name(NAME)
  public static final class ForBytes extends LessThanExpression<Bytes> {

    public ForBytes(
        @ValueParameter(clazz = Bytes.class) final Evaluation<Bytes> parameter1,
        @ValueParameter(clazz = Bytes.class) final Evaluation<Bytes> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Bytes> getParameterClass() {
      return Bytes.class;
    }

  }

  @Name(NAME)
  public static final class ForDate extends LessThanExpression<Date> {

    public ForDate(
        @ValueParameter(clazz = Date.class) final Evaluation<Date> parameter1,
        @ValueParameter(clazz = Date.class) final Evaluation<Date> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Date> getParameterClass() {
      return Date.class;
    }

  }

  @Name(NAME)
  public static final class ForDecimal4 extends LessThanExpression<Float> {

    public ForDecimal4(
        @ValueParameter(clazz = Float.class) final Evaluation<Float> parameter1,
        @ValueParameter(clazz = Float.class) final Evaluation<Float> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Float> getParameterClass() {
      return Float.class;
    }

  }

  @Name(NAME)
  public static final class ForDecimal8 extends LessThanExpression<Double> {

    public ForDecimal8(
        @ValueParameter(clazz = Double.class) final Evaluation<Double> parameter1,
        @ValueParameter(clazz = Double.class) final Evaluation<Double> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Double> getParameterClass() {
      return Double.class;
    }

  }

  @Name(NAME)
  public static final class ForDecimal extends LessThanExpression<BigDecimal> {

    public ForDecimal(
        @ValueParameter(clazz = BigDecimal.class) final Evaluation<BigDecimal> parameter1,
        @ValueParameter(clazz = BigDecimal.class) final Evaluation<BigDecimal> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<BigDecimal> getParameterClass() {
      return BigDecimal.class;
    }

  }

  @Name(NAME)
  public static final class ForInteger1 extends LessThanExpression<Byte> {

    public ForInteger1(
        @ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter1,
        @ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Byte> getParameterClass() {
      return Byte.class;
    }

  }

  @Name(NAME)
  public static final class ForInteger2 extends LessThanExpression<Short> {

    public ForInteger2(
        @ValueParameter(clazz = Short.class) final Evaluation<Short> parameter1,
        @ValueParameter(clazz = Short.class) final Evaluation<Short> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Short> getParameterClass() {
      return Short.class;
    }

  }

  @Name(NAME)
  public static final class ForInteger4 extends LessThanExpression<Integer> {

    public ForInteger4(
        @ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter1,
        @ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Integer> getParameterClass() {
      return Integer.class;
    }

  }

  @Name(NAME)
  public static final class ForInteger8 extends LessThanExpression<Long> {

    public ForInteger8(
        @ValueParameter(clazz = Long.class) final Evaluation<Long> parameter1,
        @ValueParameter(clazz = Long.class) final Evaluation<Long> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Long> getParameterClass() {
      return Long.class;
    }

  }

  @Name(NAME)
  public static final class ForInteger extends LessThanExpression<BigInteger> {

    public ForInteger(
        @ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter1,
        @ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<BigInteger> getParameterClass() {
      return BigInteger.class;
    }

  }

  @Name(NAME)
  public static final class ForString extends LessThanExpression<String> {

    public ForString(
        @ValueParameter(clazz = String.class) final Evaluation<String> parameter1,
        @ValueParameter(clazz = String.class) final Evaluation<String> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<String> getParameterClass() {
      return String.class;
    }

  }

  @Name(NAME)
  public static final class ForTimestamp extends LessThanExpression<Timestamp> {

    public ForTimestamp(
        @ValueParameter(clazz = Timestamp.class) final Evaluation<Timestamp> parameter1,
        @ValueParameter(clazz = Timestamp.class) final Evaluation<Timestamp> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Timestamp> getParameterClass() {
      return Timestamp.class;
    }

  }

  @Name(NAME)
  public static final class ForTime extends LessThanExpression<Time> {

    public ForTime(
        @ValueParameter(clazz = Time.class) final Evaluation<Time> parameter1,
        @ValueParameter(clazz = Time.class) final Evaluation<Time> parameter2
    ) {
      super(parameter1, parameter2);
    }

    @Nonnull
    @Override
    public Class<Time> getParameterClass() {
      return Time.class;
    }

  }

}
