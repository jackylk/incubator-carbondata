package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;
import com.huawei.cloudtable.leo.expression.RangeExpression;
import com.huawei.cloudtable.leo.value.Bytes;
import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;

@Function.Name(InArrayExpression.NAME)
public abstract class InArrayExpression<TParameter extends Comparable<TParameter>> extends Evaluation<Boolean>
  implements Function<Boolean>, RangeExpression<TParameter> {

  public static final String NAME = "IN";

  private final Identifier name;

  private final Evaluation<TParameter>[] arrayParameter;

  private final Evaluation<TParameter> valueParameter;

  private Evaluator<Boolean> evaluator;

  private final Set<TParameter> valueSet;

  public InArrayExpression(final Evaluation<TParameter> valueParameter, final Evaluation<TParameter>[] arrayParameter) {
    super(Boolean.class, false, toArray(valueParameter, arrayParameter));
    if (valueParameter == null) {
      // TODO throw parameter illegal exception.
      throw new UnsupportedOperationException();
    }
    name = Function.getName(getClass());
    this.valueParameter = valueParameter;
    this.arrayParameter = arrayParameter;

    final Set<TParameter> valueSet = new HashSet<>(arrayParameter.length);
    for (int index = 0; index < arrayParameter.length; index++) {
      try {
        valueSet.add(arrayParameter[index].evaluate(null));
      } catch (EvaluateException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }
    this.valueSet = valueSet;
  }

  @Override
  public Boolean evaluate(RuntimeContext context) throws EvaluateException {
    final TParameter value = valueParameter.evaluate(context);
    return valueSet.contains(value);
  }

  @Override
  public ValueRange<TParameter> getValueRange(int variableIndex) {
    final Iterator<TParameter> arrayIterator = valueSet.iterator();
    final TParameter firstValue = arrayIterator.next();
    ValueRange<TParameter> valueRange = ValueRange.of(firstValue);
    while (arrayIterator.hasNext()) {
      valueRange = valueRange.or(ValueRange.Interval.of(arrayIterator.next()));
    }
    return valueRange;
  }

  @Override
  public ValueRange<TParameter> getValueRange(int variableIndex, RuntimeContext context) {
    return null;
  }

  @Nonnull
  @Override
  public Identifier getName() {
    return name;
  }

  private Evaluation<TParameter> getValueParameter() {
    return super.getParameter(0);
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.getValueParameter().toString(stringBuilder);
    stringBuilder.append(" ").append(NAME).append(" ");
    stringBuilder.append("(");
    for (int parameterIndex = 0; parameterIndex < this.arrayParameter.length; parameterIndex++) {
      if (parameterIndex != 0) {
        stringBuilder.append(", ");
      }
      this.arrayParameter[parameterIndex].toString(stringBuilder);
    }
    stringBuilder.append(")");
  }

  private static Evaluation[] toArray(final Evaluation valueParameter, final Evaluation[] arrayParameter) {
    if (arrayParameter.length == 0) {
      return new Evaluation[]{valueParameter};
    } else {
      final Evaluation[] evaluations = new Evaluation[arrayParameter.length + 1];
      evaluations[0] = valueParameter;
      System.arraycopy(arrayParameter, 0, evaluations, 1, arrayParameter.length);
      return evaluations;
    }
  }

  @Name(NAME)
  public static final class ForBoolean extends InArrayExpression<Boolean> {
    public ForBoolean(
      @ValueParameter(clazz = Boolean.class) final Evaluation<Boolean> valueParameter,
      @ArrayParameter(clazz = Boolean[].class) final Evaluation<Boolean>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForBytes extends InArrayExpression<Bytes> {
    public ForBytes(
      @ValueParameter(clazz = Bytes.class) final Evaluation<Bytes> valueParameter,
      @ArrayParameter(clazz = Bytes[].class) final Evaluation<Bytes>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForDate extends InArrayExpression<Date> {
    public ForDate(
      @ValueParameter(clazz = Date.class) final Evaluation<Date> valueParameter,
      @ArrayParameter(clazz = Date[].class) final Evaluation<Date>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForDecimal4 extends InArrayExpression<Float> {
    public ForDecimal4(
      @ValueParameter(clazz = Float.class) final Evaluation<Float> valueParameter,
      @ArrayParameter(clazz = Float[].class) final Evaluation<Float>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForDecimal8 extends InArrayExpression<Double> {
    public ForDecimal8(
      @ValueParameter(clazz = Double.class) final Evaluation<Double> valueParameter,
      @ArrayParameter(clazz = Double[].class) final Evaluation<Double>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForDecimal extends InArrayExpression<BigDecimal> {
    public ForDecimal(
      @ValueParameter(clazz = BigDecimal.class) final Evaluation<BigDecimal> valueParameter,
      @ArrayParameter(clazz = BigDecimal[].class) final Evaluation<BigDecimal>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForInteger1 extends InArrayExpression<Byte> {
    public ForInteger1(
      @ValueParameter(clazz = Byte.class) final Evaluation<Byte> valueParameter,
      @ArrayParameter(clazz = Byte[].class) final Evaluation<Byte>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForInteger2 extends InArrayExpression<Short> {
    public ForInteger2(
      @ValueParameter(clazz = Short.class) final Evaluation<Short> valueParameter,
      @ArrayParameter(clazz = Short[].class) final Evaluation<Short>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForInteger4 extends InArrayExpression<Integer> {
    public ForInteger4(
      @ValueParameter(clazz = Integer.class) final Evaluation<Integer> valueParameter,
      @ArrayParameter(clazz = Integer[].class) final Evaluation<Integer>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForInteger8 extends InArrayExpression<Long> {
    public ForInteger8(
      @ValueParameter(clazz = Long.class) final Evaluation<Long> valueParameter,
      @ArrayParameter(clazz = Long[].class) final Evaluation<Long>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForInteger extends InArrayExpression<BigInteger> {
    public ForInteger(
      @ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> valueParameter,
      @ArrayParameter(clazz = BigInteger[].class) final Evaluation<BigInteger>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForString extends InArrayExpression<String> {
    public ForString(
      @ValueParameter(clazz = String.class) final Evaluation<String> valueParameter,
      @ArrayParameter(clazz = String[].class) final Evaluation<String>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForTimestamp extends InArrayExpression<Timestamp> {
    public ForTimestamp(
      @ValueParameter(clazz = Timestamp.class) final Evaluation<Timestamp> valueParameter,
      @ArrayParameter(clazz = Timestamp[].class) final Evaluation<Timestamp>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

  @Name(NAME)
  public static final class ForTime extends InArrayExpression<Time> {
    public ForTime(
      @ValueParameter(clazz = Time.class) final Evaluation<Time> valueParameter,
      @ArrayParameter(clazz = Time[].class) final Evaluation<Time>[] arrayParameter) {
      super(valueParameter, arrayParameter);
    }
  }

}
