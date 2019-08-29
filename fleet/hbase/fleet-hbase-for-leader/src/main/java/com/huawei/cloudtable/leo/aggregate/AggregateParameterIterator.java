/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.aggregate;

import com.huawei.cloudtable.leo.HBaseTableReferencePart;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.Aggregator;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;

/**
 * 功能描述
 *
 * @author z00337730
 * @since 2019-06-20
 */
final class AggregateParameterIterator extends Aggregator.ParametersIterator {
    private final HBaseTableReferencePart tableDefinition;

    private final Aggregation<?> aggregation;

    private int parameterIndex = -1;

    private final Evaluation.RuntimeContext runtimeContext;

    AggregateParameterIterator(final HBaseTableReferencePart tableDefinition, Aggregation<?> aggregation, final
        Evaluation.RuntimeContext runtimeContext) {
        this.tableDefinition = tableDefinition;
        this.aggregation = aggregation;
        this.runtimeContext = runtimeContext;
    }

    @Override
    public boolean nextIsNull() {
        return false;
    }

    @Override
    public ValueBytes next(ValueCodec valueCodec) {
        this.parameterIndex++;
        final Evaluation<?> parameterDefinition = this.aggregation.getParameter(this.parameterIndex);
        try {
            Object object = parameterDefinition.evaluate(this.runtimeContext);
            return ValueBytes.of(valueCodec.encode(object));
        } catch (EvaluateException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return this.parameterIndex < this.aggregation.getParameterCount() - 1;
    }

    @Override
    public Object next() {
        this.parameterIndex++;
        final Evaluation<?> parameterDefinition = this.aggregation.getParameter(this.parameterIndex);
        try {
            return parameterDefinition.evaluate(this.runtimeContext);
        } catch (EvaluateException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    public void reset() {
        this.parameterIndex = -1;
    }
}
