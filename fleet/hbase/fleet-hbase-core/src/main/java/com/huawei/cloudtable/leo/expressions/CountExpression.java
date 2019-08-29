package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;

import java.nio.ByteBuffer;

@Function.Name(CountExpression.NAME)
public final class CountExpression extends Aggregation<Long> {

  public static final String NAME = "COUNT";

  public CountExpression(@ArrayParameter(clazz = Object[].class, elementNullable = true) final Evaluation... parameterList) {
    super(Long.class, false, parameterList);
    if (parameterList.length == 0) {
      throw new RuntimeException();
    }
  }

  @Override
  public Aggregator newAggregator(final Evaluation.CompileContext context) {
    return new Aggregator();
  }

  private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<Long>
      implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

    private long count = 0;

    @Override
    public Long getResult() {
      return this.count;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void aggregate(final ParametersIterator parameters) {
      while (parameters.hasNext()) {
        if (!parameters.nextIsNull()) {
          this.count++;
          return;
        }
      }
    }

    @Override
    public void aggregate(final Partition partition) {
      this.count += partition.count;
    }

    @Override
    public Partition newPartition() {
      return new Partition();
    }

    static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

      private int count;

      @Override
      public void aggregate(final Aggregator.ParametersIterator parameters) throws EvaluateException {
        if (this.count == Integer.MAX_VALUE) {
          throw new EvaluateException();// TODO (EvaluateException.Reason.INVOKE_COUNT_EXCEED_UPPER_LIMIT);
        }
        while (parameters.hasNext()) {
          if (!parameters.nextIsNull()) {
            this.count++;
            return;
          }
        }
      }

      @Override
      public void serialize(final ByteBuffer byteBuffer) {
        byteBuffer.putInt(this.count);
      }

      @Override
      public void deserialize(final ByteBuffer byteBuffer) {
        this.count = byteBuffer.getInt();
      }

    }

  }

}
