/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * 求和聚合函数
 *
 * @author z00337730
 * @param <TResult> 返回结果类型
 * @param <TParameter> 入参类型
 * @since 2019-07-01
 */
public abstract class SumExpression<TResult, TParameter> extends Aggregation<TResult> {
    /**
     * 求和方法
     */
    public static final String NAME = "SUM";

    /**
     * 对Byte进行求和
     * @since 2019-07-01
     */
    @Name(SumExpression.NAME)
    public static final class ForInteger1 extends SumExpression<BigInteger, Byte> {
        public ForInteger1(@ValueParameter(clazz = Byte.class) final Evaluation<Byte> parameter) {
            super(BigInteger.class, parameter);
        }

        /**
         * 生成Aggregartor
         * @param context
         * @return
         */
        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigInteger>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            @Override
            public BigInteger getResult() {
                return summary;
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Byte data = parameters.next(Byte.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
            }

            @Override
            public void aggregate(final Partition partition) {
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary = 0;

                @Override
                public void aggregate(final ParametersIterator parameters) throws EvaluateException {
                    final Byte data = parameters.next(Byte.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                }

            }

        }

    }

    @Name(SumExpression.NAME)
    public static final class ForInteger2 extends SumExpression<BigInteger, Short> {

        public ForInteger2(@ValueParameter(clazz = Short.class) final Evaluation<Short> parameter) {
            super(BigInteger.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigInteger>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            @Override
            public BigInteger getResult() {
                return summary;
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Short data = parameters.next(Short.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
            }

            @Override
            public void aggregate(final Partition partition) {
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary = 0;

                @Override
                public void aggregate(final ParametersIterator parameters) throws EvaluateException {
                    final Short data = parameters.next(Short.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                }

            }

        }

    }

    @Name(SumExpression.NAME)
    public static final class ForInteger4 extends SumExpression<BigInteger, Integer> {

        public ForInteger4(@ValueParameter(clazz = Integer.class) final Evaluation<Integer> parameter) {
            super(BigInteger.class, parameter);
        }

        @Override
        public Aggregator newAggregator(final Evaluation.CompileContext context) {
            return new Aggregator();
        }

        private static final class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigInteger>
            implements com.huawei.cloudtable.leo.expression.Aggregator.Partible<Aggregator.Partition> {

            private BigInteger summary = BigInteger.ZERO;

            @Override
            public BigInteger getResult() {
                return summary;
            }

            @Override
            public void aggregate(final Partition partition) {
                this.summary = this.summary.add(BigInteger.valueOf(partition.summary));
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Integer data = parameters.next(Integer.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            static final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

                private long summary = 0;

                @Override
                public void aggregate(final ParametersIterator parameters) throws EvaluateException {
                    final Integer data = parameters.next(Integer.class);
                    if (data == null) {
                        return;
                    }
                    this.summary += data;
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    byteBuffer.putLong(this.summary);
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    this.summary = byteBuffer.getLong();
                }

            }

        }

    }

    @Name(SumExpression.NAME)
    public static final class ForInteger8 extends SumExpression<BigInteger, Long> {

        public ForInteger8(@ValueParameter(clazz = Long.class) final Evaluation<Long> parameter) {
            super(BigInteger.class, parameter);
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

        private static class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigInteger> {

            BigInteger summary = BigInteger.ZERO;

            @Override
            public BigInteger getResult() {
                return summary;
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Long data = parameters.next(Long.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data));
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
                this.summary = this.summary.add(partition.summary);
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            final class Partition extends Aggregator.Partition {

                private BigInteger summary = BigInteger.ZERO;

                @Override
                public void aggregate(final ParametersIterator parameters) {
                    final Long data = parameters.next(Long.class);
                    if (data == null) {
                        return;
                    }
                    this.summary = this.summary.add(BigInteger.valueOf(data));
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = ForInteger8.AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        summaryCodec.encode(this.summary, byteBuffer);
                    } else {
                        summaryCodec.encodeWithLength(this.summary, byteBuffer);
                    }
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = ForInteger8.AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        this.summary = summaryCodec.decode(byteBuffer);
                    } else {
                        this.summary = summaryCodec.decodeWithLength(byteBuffer);
                    }
                }

            }

        }

    }

    @Name(SumExpression.NAME)
    public static final class ForInteger extends SumExpression<BigInteger, BigInteger> {

        public ForInteger(@ValueParameter(clazz = BigInteger.class) final Evaluation<BigInteger> parameter) {
            super(BigInteger.class, parameter);
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

        private static class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<BigInteger> {

            BigInteger summary = BigInteger.ZERO;

            @Override
            public BigInteger getResult() {
                return summary;
            }

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final Integer data = parameters.next(Integer.class);
                if (data == null) {
                    return;
                }
                this.summary = this.summary.add(BigInteger.valueOf(data.longValue()));
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
                this.summary = this.summary.add(partition.summary);
            }

            @Override
            public Partition newPartition() {
                return new Partition();
            }

            final class Partition extends Aggregator.Partition {

                private BigInteger summary = BigInteger.ZERO;

                @Override
                public void aggregate(final ParametersIterator parameters) throws EvaluateException {
                    final BigInteger data = parameters.next(BigInteger.class);
                    if (data == null) {
                        return;
                    }
                    this.summary = this.summary.add(data);
                }

                @Override
                public void serialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = ForInteger.AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        summaryCodec.encode(this.summary, byteBuffer);
                    } else {
                        summaryCodec.encodeWithLength(this.summary, byteBuffer);
                    }
                }

                @Override
                public void deserialize(final ByteBuffer byteBuffer) {
                    final ValueCodec<BigInteger> summaryCodec = ForInteger.AggregatorPartible.this.summaryCodec;
                    if (summaryCodec.isFixedLength()) {
                        this.summary = summaryCodec.decode(byteBuffer);
                    } else {
                        this.summary = summaryCodec.decodeWithLength(byteBuffer);
                    }
                }

            }

        }

    }

    private SumExpression(final Class<TResult> resultClass, final Evaluation<TParameter> parameter) {
        super(resultClass, true, parameter);
    }

}
