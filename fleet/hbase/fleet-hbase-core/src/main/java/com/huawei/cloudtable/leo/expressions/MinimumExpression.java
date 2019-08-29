package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.DirectGet;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.value.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class MinimumExpression<TParameter extends Comparable<TParameter>> extends Aggregation<TParameter> {

  public static final String NAME = "MIN";

  private static final byte EMPTY_TRUE = (byte) 0xFF;

  private static final byte EMPTY_FALSE = 0x00;

  private MinimumExpression(final Evaluation<TParameter> parameter) {
    super(parameter.getResultClass(), true, parameter);
  }

  @SuppressWarnings("unchecked")
  public Evaluation<TParameter> getParameter() {
    return (Evaluation<TParameter>)super.getParameter(0);
  }

  @Override
  public com.huawei.cloudtable.leo.expression.Aggregator<TParameter> newAggregator(final Evaluation.CompileContext context) {
    if (!context.hasHint(Evaluation.CompileHints.DISABLE_BYTE_COMPARE) && this.getParameter() instanceof DirectGet) {
      final ValueCodec<TParameter> parameterCodec = context.getValueCodec(this.getParameter().getResultClass());
      if (parameterCodec != null) {
        if (parameterCodec.isOrderPreserving()) {
          return new AggregatorPartible1(parameterCodec);
        } else {
          return new AggregatorPartible2(parameterCodec);
        }
      }
    }
    return new Aggregator();
  }

  @Name(NAME)
  public static final class ForBoolean extends MinimumExpression<Boolean> {

    public ForBoolean(@ValueParameter(clazz = Boolean.class) Evaluation<Boolean> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForBytes extends MinimumExpression<Bytes> {

    public ForBytes(@ValueParameter(clazz = Bytes.class) Evaluation<Bytes> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForDate extends MinimumExpression<Date> {

    public ForDate(@ValueParameter(clazz = Date.class) Evaluation<Date> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForInteger1 extends MinimumExpression<Byte> {

    public ForInteger1(@ValueParameter(clazz = Byte.class) Evaluation<Byte> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForInteger2 extends MinimumExpression<Short> {

    public ForInteger2(@ValueParameter(clazz = Short.class) Evaluation<Short> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForInteger4 extends MinimumExpression<Integer> {

    public ForInteger4(@ValueParameter(clazz = Integer.class) Evaluation<Integer> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForInteger8 extends MinimumExpression<Long> {

    public ForInteger8(@ValueParameter(clazz = Long.class) Evaluation<Long> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForInteger extends MinimumExpression<BigInteger> {

    public ForInteger(@ValueParameter(clazz = BigInteger.class) Evaluation<BigInteger> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForDecimal4 extends MinimumExpression<Float> {

    public ForDecimal4(@ValueParameter(clazz = Float.class) Evaluation<Float> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForDecimal8 extends MinimumExpression<Double> {

    public ForDecimal8(@ValueParameter(clazz = Double.class) Evaluation<Double> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForDecimal extends MinimumExpression<BigDecimal> {

    public ForDecimal(@ValueParameter(clazz = BigDecimal.class) Evaluation<BigDecimal> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForString extends MinimumExpression<String> {

    public ForString(@ValueParameter(clazz = String.class) Evaluation<String> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForTimestamp extends MinimumExpression<Timestamp> {

    public ForTimestamp(@ValueParameter(clazz = Timestamp.class) Evaluation<Timestamp> parameter) {
      super(parameter);
    }

  }

  @Name(NAME)
  public static final class ForTime extends MinimumExpression<Time> {

    public ForTime(@ValueParameter(clazz = Time.class) Evaluation<Time> parameter) {
      super(parameter);
    }

  }

  private class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<TParameter> {

    TParameter minimum;

    @Override
    public TParameter getResult() {
      return this.minimum;
    }

    @Override
    public void aggregate(final ParametersIterator parameters) {
      final TParameter parameter = parameters.next(MinimumExpression.this.getParameter().getResultClass());
      if (this.minimum == null) {
        this.minimum = parameter;
      } else {
        if (this.minimum.compareTo(parameter) > 0) {
          this.minimum = parameter;
        }
      }
    }

  }

  private final class AggregatorPartible1 extends com.huawei.cloudtable.leo.expression.Aggregator<TParameter>
      implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<AggregatorPartible1.Partition> {

    AggregatorPartible1(final ValueCodec<TParameter> parameterCodec) {
      this.parameterCodec = parameterCodec;
    }

    private final ValueCodec<TParameter> parameterCodec;

    private ValueBytes minimum;

    @Override
    public TParameter getResult() {
      if (this.minimum == null) {
        return null;
      } else {
        return this.minimum.decode(this.parameterCodec);
      }
    }

    @Override
    public void aggregate(final ParametersIterator parameters) {
      final ValueBytes parameter = parameters.next(this.parameterCodec);
      if (parameter == null) {
        return;
      }
      if (this.minimum == null || this.minimum.compareTo(parameter) > 0) {
        this.minimum = parameter.duplicate();
      }
    }

    @Override
    public void aggregate(final Partition partition) {
      if (partition.minimum == null) {
        return;
      }
      if (this.minimum == null || this.minimum.compareTo(partition.minimum) > 0) {
        this.minimum = partition.minimum.duplicate();
      }
    }

    @Override
    public Partition newPartition() {
      return new Partition();
    }

    final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

      private ValueBytes minimum;

      @Override
      public void aggregate(final ParametersIterator parameters) {
        final ValueBytes parameter = parameters.next(AggregatorPartible1.this.parameterCodec);
        if (parameter == null) {
          return;
        }
        if (this.minimum == null || this.minimum.compareTo(parameter) > 0) {
          this.minimum = parameter.duplicate();
        }
      }

      @Override
      public void serialize(final ByteBuffer byteBuffer) {
        if (this.minimum == null) {
          byteBuffer.put(EMPTY_TRUE);
          return;
        }
        byteBuffer.put(EMPTY_FALSE);

        if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
          this.minimum.write(byteBuffer);
        } else {
          this.minimum.writeWithLength(byteBuffer);
        }
      }

      @Override
      public void deserialize(final ByteBuffer byteBuffer) {
        byte isEmpty = byteBuffer.get();
        if(EMPTY_TRUE == isEmpty){
          this.minimum = null;
          return;
        }

        if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
          this.minimum = ValueBytes.read(byteBuffer);
        } else {
          this.minimum = ValueBytes.readWithLength(byteBuffer);
        }
      }

    }

  }

  private final class AggregatorPartible2 extends Aggregator
      implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<AggregatorPartible2.Partition> {

    AggregatorPartible2(final ValueCodec<TParameter> parameterCodec) {
      this.parameterCodec = parameterCodec;
    }

    private final ValueCodec<TParameter> parameterCodec;

    @Override
    public void aggregate(final Partition partition) {
      if (this.minimum == null) {
        this.minimum = partition.minimum;
      } else {
        if (this.minimum.compareTo(partition.minimum) > 0) {
          this.minimum = partition.minimum;
        }
      }
    }

    @Override
    public Partition newPartition() {
      return new Partition();
    }

    final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

      private TParameter minimum;

      @Override
      public void aggregate(final ParametersIterator parameters) {
        final TParameter parameter = parameters.next(MinimumExpression.this.getParameter().getResultClass());
        if (this.minimum == null) {
          this.minimum = parameter;
        } else {
          if (this.minimum.compareTo(parameter) > 0) {
            this.minimum = parameter;
          }
        }
      }

      @Override
      public void serialize(final ByteBuffer byteBuffer) {
        if (this.minimum == null) {
          byteBuffer.put(EMPTY_TRUE);
          return;
        }
        byteBuffer.put(EMPTY_FALSE);

        final ValueCodec<TParameter> parameterCodec = MinimumExpression.AggregatorPartible2.this.parameterCodec;
        if (parameterCodec.isFixedLength()) {
          parameterCodec.encode(this.minimum, byteBuffer);
        } else {
          final int lengthPosition = byteBuffer.position();
          byteBuffer.putInt(0);
          final int valuePosition = byteBuffer.position();
          parameterCodec.encode(this.minimum, byteBuffer);
          byteBuffer.putInt(lengthPosition, byteBuffer.position() - valuePosition);
        }
      }

      @Override
      public void deserialize(final ByteBuffer byteBuffer) {
        byte isEmpty = byteBuffer.get();
        if(EMPTY_TRUE == isEmpty){
          this.minimum = null;
          return;
        }

        final ValueCodec<TParameter> parameterCodec = MinimumExpression.AggregatorPartible2.this.parameterCodec;
        if (parameterCodec.isFixedLength()) {
          this.minimum = parameterCodec.decode(byteBuffer);
        } else {
          final int originLimit = byteBuffer.limit();
          try {
            final int length = byteBuffer.getInt();
            byteBuffer.limit(byteBuffer.position() + length);
            this.minimum = parameterCodec.decode(byteBuffer);
          } finally {
            byteBuffer.limit(originLimit);
          }
        }
      }

    }

  }

}
