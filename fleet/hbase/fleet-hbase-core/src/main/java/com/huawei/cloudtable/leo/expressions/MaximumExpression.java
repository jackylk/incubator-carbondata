package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.DirectGet;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.value.Bytes;
import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public abstract class MaximumExpression<TParameter extends Comparable<TParameter>> extends Aggregation<TParameter> {

    public static final String NAME = "MAX";

    private static final byte EMPTY_TRUE = (byte) 0xFF;

    private static final byte EMPTY_FALSE = 0x00;

    private MaximumExpression(final Evaluation<TParameter> parameter) {
        super(parameter.getResultClass(), true, parameter);
    }

    @SuppressWarnings("unchecked")
    public Evaluation<TParameter> getParameter() {
        return (Evaluation<TParameter>) super.getParameter(0);
    }

    @Override
    public com.huawei.cloudtable.leo.expression.Aggregator<TParameter> newAggregator(
        final Evaluation.CompileContext context) {
        if (!context.hasHint(Evaluation.CompileHints.DISABLE_BYTE_COMPARE)
            && this.getParameter() instanceof DirectGet) {
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
    public static final class ForBoolean extends MaximumExpression<Boolean> {

        public ForBoolean(@ValueParameter(clazz = Boolean.class) Evaluation<Boolean> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForBytes extends MaximumExpression<Bytes> {

        public ForBytes(@ValueParameter(clazz = Bytes.class) Evaluation<Bytes> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForDate extends MaximumExpression<Date> {

        public ForDate(@ValueParameter(clazz = Date.class) Evaluation<Date> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForInteger1 extends MaximumExpression<Byte> {

        public ForInteger1(@ValueParameter(clazz = Byte.class) Evaluation<Byte> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForInteger2 extends MaximumExpression<Short> {

        public ForInteger2(@ValueParameter(clazz = Short.class) Evaluation<Short> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForInteger4 extends MaximumExpression<Integer> {

        public ForInteger4(@ValueParameter(clazz = Integer.class) Evaluation<Integer> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForInteger8 extends MaximumExpression<Long> {

        public ForInteger8(@ValueParameter(clazz = Long.class) Evaluation<Long> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForInteger extends MaximumExpression<BigInteger> {

        public ForInteger(@ValueParameter(clazz = BigInteger.class) Evaluation<BigInteger> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForDecimal4 extends MaximumExpression<Float> {

        public ForDecimal4(@ValueParameter(clazz = Float.class) Evaluation<Float> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForDecimal8 extends MaximumExpression<Double> {

        public ForDecimal8(@ValueParameter(clazz = Double.class) Evaluation<Double> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForDecimal extends MaximumExpression<BigDecimal> {

        public ForDecimal(@ValueParameter(clazz = BigDecimal.class) Evaluation<BigDecimal> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForString extends MaximumExpression<String> {

        public ForString(@ValueParameter(clazz = String.class) Evaluation<String> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForTimestamp extends MaximumExpression<Timestamp> {

        public ForTimestamp(@ValueParameter(clazz = Timestamp.class) Evaluation<Timestamp> parameter) {
            super(parameter);
        }

    }

    @Name(NAME)
    public static final class ForTime extends MaximumExpression<Time> {

        public ForTime(@ValueParameter(clazz = Time.class) Evaluation<Time> parameter) {
            super(parameter);
        }

    }

    private class Aggregator extends com.huawei.cloudtable.leo.expression.Aggregator<TParameter> {

        TParameter maximum;

        @Override
        public TParameter getResult() {
            return this.maximum;
        }

        @Override
        public void aggregate(final ParametersIterator parameters) {
            final TParameter parameter = parameters.next(MaximumExpression.this.getParameter().getResultClass());
            if (this.maximum == null) {
                this.maximum = parameter;
                if (parameter != null) {
                }
            } else {
                if (this.maximum.compareTo(parameter) < 0) {
                    this.maximum = parameter;
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

        private ValueBytes maximum;

        private boolean isNull = true;

        @Override
        public TParameter getResult() {
            if (this.maximum == null) {
                return null;
            } else {
                return this.maximum.decode(this.parameterCodec);
            }
        }

        @Override
        public void aggregate(final ParametersIterator parameters) {
            final ValueBytes parameter = parameters.next(this.parameterCodec);
            if (parameter == null) {
                return;
            }
            if (this.maximum == null || this.maximum.compareTo(parameter) < 0) {
                this.maximum = parameter.duplicate();
                isNull = false;
            }
        }

        @Override
        public void aggregate(final Partition partition) {
            if (partition.maximum == null) {
                return;
            }
            if (this.maximum == null || this.maximum.compareTo(partition.maximum) < 0) {
                this.maximum = partition.maximum.duplicate();
                isNull = false;
            }
        }

        @Override
        public Partition newPartition() {
            return new Partition();
        }

        final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

            private ValueBytes maximum;

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final ValueBytes parameter = parameters.next(AggregatorPartible1.this.parameterCodec);
                if (parameter == null) {
                    return;
                }
                if (this.maximum == null || this.maximum.compareTo(parameter) < 0) {
                    this.maximum = parameter.duplicate();
                }
            }

            @Override
            public void serialize(final ByteBuffer byteBuffer) {
                if (this.maximum == null) {
                    byteBuffer.put(EMPTY_TRUE);
                    return;
                }
                byteBuffer.put(EMPTY_FALSE);
                if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
                    this.maximum.write(byteBuffer);
                } else {
                    this.maximum.writeWithLength(byteBuffer);
                }
            }

            @Override
            public void deserialize(final ByteBuffer byteBuffer) {
                byte isEmpty = byteBuffer.get();
                if(EMPTY_TRUE == isEmpty){
                    this.maximum = null;
                    return;
                }

                if (AggregatorPartible1.this.parameterCodec.isFixedLength()) {
                    this.maximum = ValueBytes.read(byteBuffer);
                } else {
                    this.maximum = ValueBytes.readWithLength(byteBuffer);
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
            if (this.maximum == null) {
                this.maximum = partition.maximum;
            } else {
                if (this.maximum.compareTo(partition.maximum) < 0) {
                    this.maximum = partition.maximum;
                }
            }
        }

        @Override
        public Partition newPartition() {
            return new Partition();
        }

        final class Partition extends com.huawei.cloudtable.leo.expression.Aggregator.Partition {

            private TParameter maximum;

            @Override
            public void aggregate(final ParametersIterator parameters) {
                final TParameter parameter = parameters.next(MaximumExpression.this.getParameter().getResultClass());
                if (this.maximum == null) {
                    this.maximum = parameter;
                } else {
                    if (this.maximum.compareTo(parameter) < 0) {
                        this.maximum = parameter;
                    }
                }
            }

            @Override
            public void serialize(final ByteBuffer byteBuffer) {
                if (this.maximum == null) {
                    byteBuffer.put(EMPTY_TRUE);
                    return;
                }
                byteBuffer.put(EMPTY_FALSE);
                final ValueCodec<TParameter> parameterCodec = AggregatorPartible2.this.parameterCodec;
                if (parameterCodec.isFixedLength()) {
                    parameterCodec.encode(this.maximum, byteBuffer);
                } else {
                    parameterCodec.encodeWithLength(this.maximum, byteBuffer);
                }
            }

            @Override
            public void deserialize(final ByteBuffer byteBuffer) {
                byte isEmpty = byteBuffer.get();
                if(EMPTY_TRUE == isEmpty){
                    this.maximum = null;
                    return;
                }

                final ValueCodec<TParameter> parameterCodec = AggregatorPartible2.this.parameterCodec;
                if (parameterCodec.isFixedLength()) {
                    this.maximum = parameterCodec.decode(byteBuffer);
                } else {
                    this.maximum = parameterCodec.decodeWithLength(byteBuffer);
                }
            }

        }

    }

}
