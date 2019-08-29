package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.DirectGet;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;

import java.nio.ByteBuffer;

@Function.Name(FirstExpression.NAME)
public final class FirstExpression<TParameter> extends Aggregation<TParameter> {

  public static final String NAME = "FIRST";

  public FirstExpression(@ValueParameter(clazz = Object.class) final Evaluation<TParameter> parameter) {
    super(parameter.getResultClass(), true, parameter);
  }

  @SuppressWarnings("unchecked")
  public Evaluation<TParameter> getParameter() {
    return (Evaluation<TParameter>) super.getParameter(0);
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

  private class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<TParameter> {

    TParameter first;

    @Override
    public TParameter getResult() {
      return this.first;
    }

    @Override
    public void aggregate(final ParametersIterator parameters) {
      if (this.first == null) {
        this.first = parameters.next(FirstExpression.this.getParameter().getResultClass());
      }
    }

  }

  private final class AggregatorPartible1 extends com.huawei.cloudtable.leo.expression.Aggregator<TParameter>
      implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<AggregatorPartible1.Partition> {

    AggregatorPartible1(final ValueCodec<TParameter> parameterCodec) {
      this.parameterCodec = parameterCodec;
    }

    private final ValueCodec<TParameter> parameterCodec;

    private ValueBytes first;

    @Override
    public TParameter getResult() {
      if (this.first == null) {
        return null;
      } else {
        return this.first.decode(this.parameterCodec);
      }
    }

    @Override
    public void aggregate(final ParametersIterator parameters) {
      if (this.first == null) {
        final ValueBytes parameter = parameters.next(this.parameterCodec);
        if (parameter == null) {
          return;
        }
        this.first = parameter.duplicate();
      }
    }

    @Override
    public void aggregate(final AggregatorPartible1.Partition partition) {
      if (partition.first == null) {
        return;
      }
      if (this.first == null) {
        this.first = partition.first.duplicate();
      }
    }

    @Override
    public AggregatorPartible1.Partition newPartition() {
      return new AggregatorPartible1.Partition();
    }

    final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

      private ValueBytes first;

      @Override
      public void aggregate(final ParametersIterator parameters) {
        if (this.first == null) {
          final ValueBytes parameter = parameters.next(AggregatorPartible1.this.parameterCodec);
          if (parameter == null) {
            return;
          }
          this.first = parameter.duplicate();
        }
      }

      @Override
      public void serialize(final ByteBuffer byteBuffer) {
        if (this.first == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
          this.first.write(byteBuffer);
        } else {
          this.first.writeWithLength(byteBuffer);
        }
      }

      @Override
      public void deserialize(final ByteBuffer byteBuffer) {
        if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
          this.first = ValueBytes.read(byteBuffer);
        } else {
          this.first = ValueBytes.readWithLength(byteBuffer);
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
    public void aggregate(final AggregatorPartible2.Partition partition) {
      if (this.first == null) {
        this.first = partition.first;
      }
    }

    @Override
    public AggregatorPartible2.Partition newPartition() {
      return new AggregatorPartible2.Partition();
    }

    final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

      private TParameter first;

      @Override
      public void aggregate(final ParametersIterator parameters) {
        if (this.first == null) {
          final TParameter parameter = parameters.next(FirstExpression.this.getParameter().getResultClass());
          if (parameter == null) {
            return;
          }
          this.first = parameter;
        }
      }

      @Override
      public void serialize(final ByteBuffer byteBuffer) {
        if (this.first == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        final ValueCodec<TParameter> parameterCodec = AggregatorPartible2.this.parameterCodec;
        if (parameterCodec.isFixedLength()) {
          parameterCodec.encode(this.first, byteBuffer);
        } else {
          parameterCodec.encodeWithLength(this.first, byteBuffer);
        }
      }

      @Override
      public void deserialize(final ByteBuffer byteBuffer) {
        final ValueCodec<TParameter> parameterCodec = AggregatorPartible2.this.parameterCodec;
        if (parameterCodec.isFixedLength()) {
          this.first = parameterCodec.decode(byteBuffer);
        } else {
          this.first = parameterCodec.decodeWithLength(byteBuffer);
        }
      }

    }

  }

}
