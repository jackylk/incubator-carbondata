/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.aggregate;

import com.huawei.cloudtable.leo.HBaseValueCodec;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.expression.Evaluation;

import java.nio.charset.Charset;

/**
 * 用于聚合的在编译时使用的上下文
 *
 * @author z00337730
 * @since 2019-06-20
 */
public class AggregateCompileContext implements Evaluation.CompileContext {
    /**
     * 支持聚合函数的编译上下文
     */
    public static final Evaluation.CompileContext COMPILE_CONTEXT = new AggregateCompileContext();

    private AggregateCompileContext() {
    }

    @Override
    public Charset getCharset() {
        return Charset.forName("UTF-8");
    }

    @Override
    public <TValue> ValueType<TValue> getValueType(final Class<TValue> valueClass) {
        return ValueTypeManager.BUILD_IN.getValueType(valueClass);
    }

    @Override
    public <TValue> HBaseValueCodec<TValue> getValueCodec(final Class<TValue> valueClass) {
        return HBaseValueCodecManager.BUILD_IN.getValueCodec(valueClass);
    }

    @Override
    public boolean hasHint(String hintName) {
        return false;
    }
}