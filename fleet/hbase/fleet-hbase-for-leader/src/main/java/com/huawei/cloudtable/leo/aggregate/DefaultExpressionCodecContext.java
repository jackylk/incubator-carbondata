/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.aggregate;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.ExpressionCodec;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.ValueCodecManager;
import com.huawei.cloudtable.leo.expression.FunctionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 默认的系统内置的编解码器
 *
 * @author z00337730
 * @since 2019-06-20
 */
public class DefaultExpressionCodecContext {
    /**
     * 系统默认反编码工具类
     */
    public static final ExpressionCodec.DecodeContext DECODE_CONTEXT = new ExpressionDecodeContext();

    /**
     * 系统默认编码工具类
     */
    public static final ExpressionCodec.EncodeContext ENCODE_CONTEXT = new ExpressionEncodeContext();

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExpressionCodecContext.class);

    private static final int DEFAULT_ENCODE_SIZE = 10240;

    private static final ThreadLocal<ByteBuffer> BUFFER_THREAD_LOCAL = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(DEFAULT_ENCODE_SIZE);
        }
    };

    private static final int INTEGER_BYTE_LENGTH = Integer.BYTES;

    private DefaultExpressionCodecContext() {
    }

    /**
     * 系统反编译工具类
     * @since 2019-06-20
     */
    static class ExpressionDecodeContext implements ExpressionCodec.DecodeContext {
        @Override
        public ValueCodecManager getValueCodecManager() {
            return HBaseValueCodecManager.BUILD_IN;
        }

        @Override
        public FunctionManager getFunctionManager() {
            return FunctionManager.BUILD_IN;
        }
    }

    /**
     * 系统编译工具类
     * @since 2019-06-20
     */
    static class ExpressionEncodeContext implements ExpressionCodec.EncodeContext {
        @Override
        public ValueCodecManager getValueCodecManager() {
            return HBaseValueCodecManager.BUILD_IN;
        }
    }

    /**
     * 对表达式进行编码
     * @param expression 表达式对象
     * @return 返回编码后的expression byte数组
     */
    public static byte[] encode(final Expression expression) {
        ByteBuffer buffer = BUFFER_THREAD_LOCAL.get();
        buffer.clear();
        ExpressionCodec.encode(ENCODE_CONTEXT, expression, buffer);
        byte[] bytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(bytes);
        return bytes;
    }

    /**
     * 表达式列表编码
     * @param expressionList 表达式列表
     * @return 返回对表达式列表编码后的byte 数组
     */
    public static byte[] encode(final List expressionList) {
        ByteBuffer tempBuffer = BUFFER_THREAD_LOCAL.get();
        tempBuffer.clear();
        // 编码格式为：[个数（4byte）][expression[0].content][expression[x].content]
        int size = expressionList.size();

        // 总体的expression个数
        tempBuffer.putInt(size);
        // 每个expression按长度_内容的方式编码
        for (Object expression : expressionList) {
            if (expression instanceof Expression) {
                throw new IllegalArgumentException("Input parameter is not Expression type, do not support.");
            }
            // 将expression编码
            ExpressionCodec.encode(ENCODE_CONTEXT, (Expression) expression, tempBuffer);
        }
        byte[] bytes = new byte[tempBuffer.position()];
        tempBuffer.position(0);
        tempBuffer.get(bytes);
        LOG.info("Encode expressionList, byte size is, {}", bytes.length);
        return bytes;
    }

    /**
     * 表达式反编码
     * @param expressions 表达式列表编码后的列表数组
     * @return 返回编码后的表达式数组列表
     */
    public static List<Expression> decodeList(final byte[] expressions) {
        // 编码格式为：[个数（4byte）][list[0].length][list[0].bytes][list[1].length][length[1].bytes]...
        if ((expressions == null) || (expressions.length <= INTEGER_BYTE_LENGTH)) {
            throw new IllegalArgumentException("The byte size of expression list should not be null or less than 4.");
        }

        ByteBuffer tempBuffer = ByteBuffer.wrap(expressions);

        // 获得表达式长度
        int size = tempBuffer.getInt();
        if (size <= 0) {
            throw new IllegalArgumentException("The length of expression list size should not less than 0.");
        }

        List<Expression> expressionList = new ArrayList<>(size);
        // 每个expression按长度_内容的方式进行解码
        for (int i = 0; i < size; i++) {
            Expression expression = ExpressionCodec.decode(DECODE_CONTEXT, tempBuffer);
            expressionList.add(expression);
        }
        LOG.info("Decoded expressionList, size is, {}", expressionList.size());
        return expressionList;
    }

    /**
     * 实现表达式的反编码
     * @param expression 表达式byte数组
     * @return 返回表达式对象
     */
    public static Expression decode(final byte[] expression) {
        if (expression == null) {
            throw new IllegalArgumentException("Input expression error.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(expression);
        return ExpressionCodec.decode(DECODE_CONTEXT, buffer);
    }
}
