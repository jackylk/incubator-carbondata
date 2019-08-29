package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.expression.Commutative;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class AdditionExpression<TResult, TParameter1, TParameter2> extends Evaluation<TResult>
    implements Function<TResult>, Commutative<TParameter1, TParameter2> {

  public static final String NAME = "+";

  @Name(NAME)
  public static final class ForInteger1 extends AdditionExpression<Short, Byte, Byte> {

    public ForInteger1(
        @ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter1,
        @ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter2
    ) {
      super(Short.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<Byte> getParameter1Class() {
      return Byte.class;
    }

    @Override
    public Class<Byte> getParameter2Class() {
      return Byte.class;
    }

    public final Evaluation<Byte> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<Byte> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public Short evaluate(final RuntimeContext context) throws EvaluateException {
      return (short) (this.getParameter1().evaluate(context) + this.getParameter2().evaluate(context));
    }

  }

  @Name(NAME)
  public static final class ForInteger2 extends AdditionExpression<Integer, Short, Short> {

    public ForInteger2(
        @ValueParameter(clazz = Short.class) final Evaluation<Short> parameter1,
        @ValueParameter(clazz = Short.class) final Evaluation<Short> parameter2
    ) {
      super(Integer.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<Short> getParameter1Class() {
      return Short.class;
    }

    @Override
    public Class<Short> getParameter2Class() {
      return Short.class;
    }

    public final Evaluation<Short> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<Short> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public Integer evaluate(final RuntimeContext context) throws EvaluateException {
      return this.getParameter1().evaluate(context) + this.getParameter2().evaluate(context);
    }

  }

  @Name(NAME)
  public static final class ForInteger4 extends AdditionExpression<Long, Integer, Integer> {

    public ForInteger4(
        @ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter1,
        @ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter2
    ) {
      super(Long.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<Integer> getParameter1Class() {
      return Integer.class;
    }

    @Override
    public Class<Integer> getParameter2Class() {
      return Integer.class;
    }

    public final Evaluation<Integer> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<Integer> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public Long evaluate(final RuntimeContext context) throws EvaluateException {
      return this.getParameter1().evaluate(context).longValue() + this.getParameter2().evaluate(context);
    }

  }

  @Name(NAME)
  public static final class ForInteger8 extends AdditionExpression<BigInteger, Long, Long> {

    public ForInteger8(
        @ValueParameter(clazz = Long.class) final Evaluation<Long> parameter1,
        @ValueParameter(clazz = Long.class) final Evaluation<Long> parameter2
    ) {
      super(BigInteger.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<Long> getParameter1Class() {
      return Long.class;
    }

    @Override
    public Class<Long> getParameter2Class() {
      return Long.class;
    }

    public final Evaluation<Long> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<Long> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public BigInteger evaluate(final RuntimeContext context) throws EvaluateException {
      return BigInteger.valueOf(this.getParameter1().evaluate(context)).add(BigInteger.valueOf(this.getParameter2().evaluate(context)));
    }

  }

  @Name(NAME)
  public static final class ForInteger extends AdditionExpression<BigInteger, BigInteger, BigInteger> {

    public ForInteger(
        @ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter1,
        @ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter2
    ) {
      super(BigInteger.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<BigInteger> getParameter1Class() {
      return BigInteger.class;
    }

    @Override
    public Class<BigInteger> getParameter2Class() {
      return BigInteger.class;
    }

    public final Evaluation<BigInteger> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<BigInteger> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public BigInteger evaluate(final RuntimeContext context) throws EvaluateException {
      return this.getParameter1().evaluate(context).add(this.getParameter2().evaluate(context));
    }

  }

  @Name(NAME)
  public static final class ForDecimal extends AdditionExpression<BigDecimal, BigDecimal, BigDecimal> {

    public ForDecimal(
      @ValueParameter(clazz = BigDecimal.class) final Evaluation<BigDecimal> parameter1,
      @ValueParameter(clazz = BigDecimal.class) final Evaluation<BigDecimal> parameter2
    ) {
      super(BigDecimal.class, parameter1, parameter2);
      if (parameter1 == null || parameter2 == null) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public Class<BigDecimal> getParameter1Class() {
      return BigDecimal.class;
    }

    @Override
    public Class<BigDecimal> getParameter2Class() {
      return BigDecimal.class;
    }

    public final Evaluation<BigDecimal> getParameter1() {
      return super.getParameter(0);
    }

    public final Evaluation<BigDecimal> getParameter2() {
      return super.getParameter(1);
    }

    @Override
    public BigDecimal evaluate(final RuntimeContext context) throws EvaluateException {
      return this.getParameter1().evaluate(context).add(this.getParameter2().evaluate(context));
    }

  }

  private AdditionExpression(
      final Class<TResult> resultClass,
      final Evaluation<TParameter1> parameter1,
      final Evaluation<TParameter2> parameter2
  ) {
    super(resultClass, false, parameter1, parameter2);
    this.name = Function.getName(this.getClass());
  }

  private final Identifier name;

  @Nonnull
  @Override
  public Identifier getName() {
    return this.name;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    Function.toString(this, stringBuilder);
  }

}
