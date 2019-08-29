package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

public abstract class AverageExpression<TResult, TParameter> extends Aggregation<TResult> {

    public static final String NAME = "AVG";

    @Name(AverageExpression.NAME)
    public static final class ForInteger1 extends AverageExpression<Float, Byte> {

        public ForInteger1(@ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter) {
            super(Float.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<Float>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            private long count;

            @Override
            public Float getResult() {
                if (this.count == 0) {
                    return null;
                } else {
                    return new BigDecimal(this.summary).divide(BigDecimal.valueOf(this.count), RoundingMode.HALF_UP)
                        .floatValue();// TODO RoundingMode
                }
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Byte data = parameters.next(Byte.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
                this.count++;
            }

            @Override
            public void aggregate(final Partition partition) {
                if (partition.count == 0) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
                this.count += partition.count;
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary;

                private int count;

                @Override
                public void aggregate(final Aggregator.ParametersIterator parameters) throws EvaluateException {
                    if (this.count == Integer.MAX_VALUE) {
                        throw new EvaluateException();// TODO (EvaluateException.Reason.INVOKE_COUNT_EXCEED_UPPER_LIMIT);
                    }
                    final Byte data = parameters.next(Byte.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                    this.count++;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                    byteBuffer.putInt(this.count);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                    this.count = byteBuffer.getInt();
                }

            }

        }

    }

    @Name(AverageExpression.NAME)
    public static final class ForInteger2 extends AverageExpression<Float, Short> {

        public ForInteger2(@ValueParameter(clazz = Short.class) final Evaluation<Short> parameter) {
            super(Float.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<Float>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            private long count;

            @Override
            public Float getResult() {
                if (this.count == 0) {
                    return null;
                } else {
                    return new BigDecimal(this.summary).divide(BigDecimal.valueOf(this.count), RoundingMode.HALF_UP)
                        .floatValue();// TODO RoundingMode
                }
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Short data = parameters.next(Short.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
                this.count++;
            }

            @Override
            public void aggregate(final Partition partition) {
                if (partition.count == 0) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
                this.count += partition.count;
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary;

                private int count;

                @Override
                public void aggregate(final Aggregator.ParametersIterator parameters) throws EvaluateException {
                    if (this.count == Integer.MAX_VALUE) {
                        throw new EvaluateException();// TODO (EvaluateException.Reason.INVOKE_COUNT_EXCEED_UPPER_LIMIT);
                    }
                    final Short data = parameters.next(Short.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                    this.count++;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                    byteBuffer.putInt(this.count);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                    this.count = byteBuffer.getInt();
                }

            }

        }

    }

    @Name(AverageExpression.NAME)
    public static final class ForInteger4 extends AverageExpression<Float, Integer> {

        public ForInteger4(@ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter) {
            super(Float.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<Float>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            private long count;

            @Override
            public Float getResult() {
                if (this.count == 0) {
                    return null;
                } else {
                    return new BigDecimal(this.summary).divide(BigDecimal.valueOf(this.count), RoundingMode.HALF_UP)
                        .floatValue();// TODO RoundingMode
                }
            }

            @Override
            public void aggregate(final Partition partition) {
                if (partition.count == 0) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
                this.count += partition.count;
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Integer data = parameters.next(Integer.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
                this.count++;
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary;

                private int count;

                @Override
                public void aggregate(final Aggregator.ParametersIterator parameters) throws EvaluateException {
                    if (this.count == Integer.MAX_VALUE) {
                        throw new EvaluateException();// TODO (EvaluateException.Reason.INVOKE_COUNT_EXCEED_UPPER_LIMIT);
                    }
                    final Integer data = parameters.next(Integer.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                    this.count++;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                    byteBuffer.putInt(this.count);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                    this.count = byteBuffer.getInt();
                }

            }

        }

    }

    @Name(AverageExpression.NAME)
    public static final class ForInteger8 extends AverageExpression<Double, Long> {

        public ForInteger8(@ValueParameter(clazz = Long.class) final Evaluation<Long> parameter) {
            super(Double.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            final ValueCodec<BigInteger> summaryCodec = context.getValueCodec(BigInteger.class);
            if (summaryCodec == null) {
                return new Aggregator();
            } else {
                return new AggregatorPartible(summaryCodec);
            }
        }

        private static class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<Double> {

            BigInteger summary = BigInteger.ZERO;

            long count;

            @Override
            public Double getResult() {
                if (this.count == 0) {
                    return null;
                } else {
                    return new BigDecimal(this.summary).divide(BigDecimal.valueOf(this.count), RoundingMode.HALF_UP)
                        .doubleValue();// TODO RoundingMode
                }
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Long data = parameters.next(Long.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data));
                this.count++;
            }

        }

        private static final class AggregatorPartible extends Aggregator
            implements Aggregator.Partible<AggregatorPartible.Partition> {

            AggregatorPartible(final ValueCodec<BigInteger> summaryCodec) {
                this.summaryCodec = summaryCodec;
            }

            private final ValueCodec<BigInteger> summaryCodec;

            @Override
            public void aggregate(final Partition partition) {
                if (partition.count == 0) {
                    return;
                }
                this.summary = this.summary.add(partition.summary);
                this.count += partition.count;
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            final class Partition extends Aggregator.Partition {

                private BigInteger summary = BigInteger.ZERO;

                private int count;

                @Override
                public void aggregate(final Aggregator.ParametersIterator parameters) {
                    final Long data = parameters.next(Long.class);
                    if (data == null) {
                        return;
                    }
                    this.summary = this.summary.add(BigInteger.valueOf(data));
                    this.count++;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        summaryCodec.encode(this.summary, byteBuffer);
                    } else {
                        summaryCodec.encodeWithLength(this.summary, byteBuffer);
                    }
                    byteBuffer.putInt(this.count);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        this.summary = summaryCodec.decode(byteBuffer);
                    } else {
                        this.summary = summaryCodec.decodeWithLength(byteBuffer);
                    }
                    this.count = byteBuffer.getInt();
                }

            }

        }

    }

    @Name(AverageExpression.NAME)
    public static final class ForInteger extends AverageExpression<BigDecimal, BigInteger> {

        public ForInteger(@ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter) {
            super(BigDecimal.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            final ValueCodec<BigInteger> summaryCodec = context.getValueCodec(BigInteger.class);
            if (summaryCodec == null) {
                return new Aggregator();
            } else {
                return new AggregatorPartible(summaryCodec);
            }
        }

        private static class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigDecimal> {

            BigInteger summary = BigInteger.ZERO;

            long count;

            @Override
            public BigDecimal getResult() {
                if (this.count == 0) {
                    return null;
                } else {
                    return new BigDecimal(this.summary).divide(BigDecimal.valueOf(this.count),
                        RoundingMode.HALF_UP);// TODO RoundingMode
                }
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Integer data = parameters.next(Integer.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
                this.count++;
            }

        }

        private static final class AggregatorPartible extends ForInteger.Aggregator
            implements Aggregator.Partible<AggregatorPartible.Partition> {

            AggregatorPartible(final ValueCodec<BigInteger> summaryCodec) {
                this.summaryCodec = summaryCodec;
            }

            private final ValueCodec<BigInteger> summaryCodec;

            @Override
            public void aggregate(final Partition partition) {
                if (partition.count == 0) {
                    return;
                }
                this.summary = this.summary.add(partition.summary);
                this.count += partition.count;
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            final class Partition extends Aggregator.Partition {

                private BigInteger summary;

                private int count;

                @Override
                public void aggregate(final Aggregator.ParametersIterator parameters) throws EvaluateException {
                    if (this.count == Integer.MAX_VALUE) {
                        throw new EvaluateException();// TODO (EvaluateException.Reason.INVOKE_COUNT_EXCEED_UPPER_LIMIT);
                    }
                    final BigInteger data = parameters.next(BigInteger.class);
                    if (data == null) {
                        return;
                    }
                    this.summary = this.summary.add(data);
                    this.count++;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        summaryCodec.encode(this.summary, byteBuffer);
                    } else {
                        summaryCodec.encodeWithLength(this.summary, byteBuffer);
                    }
                    byteBuffer.putLong(this.count);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        this.summary = summaryCodec.decode(byteBuffer);
                    } else {
                        this.summary = summaryCodec.decodeWithLength(byteBuffer);
                    }
                    this.count = byteBuffer.getInt();
                }

            }

        }

    }

    private AverageExpression(final Class<TResult> resultClass, final Evaluation<TParameter> parameter) {
        super(resultClass, true, parameter);
    }

}
