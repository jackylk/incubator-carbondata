
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.planners;

import com.huawei.cloudtable.leo.ExecuteParameters;
import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.ExpressionCodec;
import com.huawei.cloudtable.leo.HBaseExecuteEngine;
import com.huawei.cloudtable.leo.HBaseRowKeyCodec;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.HBaseTableReferenceCodec;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.aggregate.AggregateCompileContext;
import com.huawei.cloudtable.leo.aggregate.DefaultExpressionCodecContext;
import com.huawei.cloudtable.leo.aggregate.LeoAggregateProtos.LeoAggregateRequest;
import com.huawei.cloudtable.leo.aggregate.LeoAggregateProtos.LeoAggregateResponse;
import com.huawei.cloudtable.leo.aggregate.LeoAggregateProtos.LeoAggregateService;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.Aggregator;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.hbase.filters.ConditionFilter;
import com.huawei.cloudtable.leo.optimize.IndexPrefixMatchResult;

import com.google.protobuf.ByteString;
import com.sun.jdi.InternalException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于HBase的扫描结果分析迭代器
 * 当前支持
 * Select max(c1),min(c3) from table where rokey > xxx, rowkey < xxx的查询场景
 * <p>
 * 典型的客户使用场景：select sum(item1) from table1 where ts>　ts1 and ts<　ts2 and dm=xxx group by ts
 *
 * @author z00337730
 * @since 2019-06-20
 */
public class ResultAggregateIterator extends SelectPlanner.ResultIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ResultAggregateIterator.class);

    /**
     * 在当前版本，由于暂时不支持GroupBy，因此返回的消息体个数为1，因此默认的长度为1
     * 当支持GroupBy能力之后，该capacity需要修改
     */
    private static final int INITIAL_CAPACITY = 1;

    private static final int DEFAULT_CURSOR_POSITION = -1;

    private static final int DEFAULT_GROUP_SIZE = 1;

    private static final int DEFAULT_AGGREGATE_CODEC_SIZE = 10240;

    private static final ThreadLocal<ByteBuffer> BUFFER_THREAD_LOCAL = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(DEFAULT_AGGREGATE_CODEC_SIZE);
        }
    };

    private static final int MAX_RETRY_TIMES = 3;

    private final List<? extends Aggregation<?>> aggregationList;

    private final HBaseTableReference tableReference;

    private final List<Evaluation<?>> groupByEvaluations;

    private final HBaseRowKeyCodec primaryKeyCodec;

    private final Map<ByteString, List<Aggregator>> groupResult;

    private final List<IndexPrefixMatchResult> primaryKeyMatchResultList;

    private List<List<Object>> resultList;

    private int cursor = DEFAULT_CURSOR_POSITION;

    private boolean isInit = false;

    ResultAggregateIterator(final HBaseExecuteEngine executeEngine, final HBaseTableReference tableReference,
        final ExecuteParameters executeParameters, final Schema schema,
        final List<IndexPrefixMatchResult> primaryKeyMatchResultList,
        final List<? extends Aggregation<?>> aggregationList, final List<Evaluation<?>> groupByEvaluations) {
        super(executeEngine, executeParameters, schema);
        this.tableReference = tableReference;
        this.aggregationList = aggregationList;
        this.primaryKeyMatchResultList = primaryKeyMatchResultList;
        this.groupByEvaluations = groupByEvaluations;
        this.primaryKeyCodec = this.tableReference.getPrimaryKeyCodec();
        groupResult = new HashMap<>(DEFAULT_GROUP_SIZE);
        resultList = new ArrayList<>(INITIAL_CAPACITY);
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(final int attributeIndex) {
        // 有效性检查，如果发现游标为-1，则认为没有调用next方法，或者next方法返回错误
        if (cursor == DEFAULT_CURSOR_POSITION) {
            throw new IndexOutOfBoundsException("Can't get any valid result.");
        }

        // 非空检查和长度检查
        List<Object> row = resultList.get(cursor);
        if (row == null) {
            return null;
        }
        if (attributeIndex >= row.size()) {
            throw new IllegalArgumentException(
                "AttributeIndex wrong, attributeIndex should be less than " + resultList.size());
        }

        return row.get(attributeIndex);
    }

    @Override
    public boolean next() {
        synchronized (this) {
            if (!isInit) {
                // 如果rowkey返回存在多个，则需要执行多次的scan，并将多次的scan结果组合
                for (IndexPrefixMatchResult indexPrefixMatchResult : primaryKeyMatchResultList) {
                    SelectPlanner.PrimaryKeyValueIterator primaryKeyValueIterator
                        = SelectPlanner.PrimaryKeyValueIterator.CACHE.get();
                    ValueRange.Interval interval = indexPrefixMatchResult.getScanInterval();
                    byte[] startRowKey = new byte[0];
                    if (interval.getMinimum() != null) {
                        startRowKey = buildPrimaryKey(indexPrefixMatchResult, primaryKeyValueIterator,
                            ValueRange.IntervalBoundary.MINIMUM);
                    }

                    byte[] endRowKey = new byte[0];
                    if (interval.getMaximum() != null) {
                        endRowKey = buildPrimaryKey(indexPrefixMatchResult, primaryKeyValueIterator,
                            ValueRange.IntervalBoundary.MAXIMUM);
                    }

                    int retryTimes = 0;
                    boolean isNeedRetry = false;
                    do {
                        try {
                            executeAggregate(startRowKey, interval.isIncludeMinimum(), endRowKey,
                                interval.isIncludeMaximum(), indexPrefixMatchResult.getFilterCondition());
                        } catch (IOException e) {
                            retryTimes++;
                            isNeedRetry = true;
                            LOG.error("ExecuteAggregate retry times {}.", retryTimes);
                        }
                    } while ((retryTimes < MAX_RETRY_TIMES) && isNeedRetry);
                }

                // result中的值，实际上一行的记录，因为当前版本，不支持Group，因此从coprocessor中返回的值，只有一行
                // 此处试将多个聚合语句的结果拼接成一行
                for (ByteString groupKey : groupResult.keySet()) {
                    List<Object> result = new ArrayList<>(aggregationList.size());
                    for (Aggregator aggregator : groupResult.get(groupKey)) {
                        result.add(aggregator.getResult());
                    }
                    resultList.add(result);
                }

                LOG.info("Execute aggregate result size is: {}", resultList.size());
                isInit = true;
            }
        }

        int size = resultList.size();
        if ((size > 0) && ((size - 1) > cursor)) {
            cursor++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        this.resultList.clear();
        this.cursor = DEFAULT_CURSOR_POSITION;
    }

    @Override
    SelectPlanner.ResultIterator duplicate(final ExecuteParameters executeParameters) {
        return new ResultAggregateIterator(this.executeEngine, this.tableReference, this.executeParameters,
            this.getSchema(), this.primaryKeyMatchResultList, this.aggregationList, this.groupByEvaluations);
    }

    /**
     * 根据不同的groupkey，生成Aggregatorlist对象。
     * 每个Aggregator实际上是一个聚合算法
     * 不同的groupkey，需要有不同的AggregatorList算法
     *
     * @param groupKey
     * @return
     */
    private List<Aggregator> getGroupAggregatorList(ByteString groupKey) {
        if (!groupResult.containsKey(groupKey)) {
            List<Aggregator> groupAggregatorList = new ArrayList<>(aggregationList.size());
            for (Aggregation aggregation : aggregationList) {
                Aggregator aggregator = aggregation.newAggregator(AggregateCompileContext.COMPILE_CONTEXT);
                groupAggregatorList.add(aggregator);
            }
            groupResult.put(groupKey, groupAggregatorList);
        }

        return groupResult.get(groupKey);
    }

    private static boolean checkNull(byte[] object) {
        if ((object == null) || (object.length == 0)) {
            return true;
        }
        return false;
    }

    /**
     * 通过HBase的Corprocessor机制来计算汇聚结果
     * 使用EndPointCorprocessor，而不是使用OvserverCorprocessor，目的是请求分发到所有的Region上执行，而不是在一个RegionServer上串行执行
     * 该方法可被多次调用，每次调用的结果会按groupid进行分组组合
     * 由于结果都在内存中，因此结果值不能太大
     *
     * @return
     */
    private void executeAggregate(byte[] startRowKey, boolean isIncludeStartRowKey, byte[] endRowKey,
        boolean isIncludeEndRowKey, Evaluation<Boolean> condition) throws IOException {
        LOG.info("Execute aggregate, startRowKey={},endRowKey={}", startRowKey, endRowKey);
        // 拼装消息体
        // 消息体的内容：
        // 1.Scan的范围（startrowkey,endrowkey）
        // 2.要查询的列（ColumnList，转换成二进制，List<String>）
        // 3.要执行的Expression算法列表，一个Column对应一一个Expression对象，二进制字节码
        // 对于Start_rowKey和EndRowkey，需要在两个地方传递，
        // 一个是传递给CoprocessorsService的时候，可以通过rowkey范围确定要发送的Region范围
        // 一个是需要继续传递给具体的Region，因为Coprocessor本身是RPC调用，会将请求直接传递到具体的Region中
        // 因此需要在Region中通过rowkey范围进一步确定范围

        // 封装rowkey范围
        LeoAggregateRequest.Builder builder = getBuilderWithRowKey(startRowKey, isIncludeStartRowKey, endRowKey,
            isIncludeEndRowKey);

        // 封装Table定义
        byte[] tableReferenceBytes = getTableReference(condition);
        builder.setTableDefine(ByteString.copyFrom(tableReferenceBytes));

        // 封装条件查询，编码后传递
        if (condition != null) {
            ConditionFilter conditionFilter = new ConditionFilter(this.tableReference, condition);
            builder.setFilter(ByteString.copyFrom(conditionFilter.toByteArray()));
        }

        // 封装groupby请求
        if (groupByEvaluations != null) {
            for (Evaluation<?> groupByEvaluation : groupByEvaluations) {
                byte[] groupExpression = DefaultExpressionCodecContext.encode(groupByEvaluation);
                builder.getGroupbyBuilder().addExpression(ByteString.copyFrom(groupExpression));
            }
        }

        // 封装Aggregation条件语句
        for (Aggregation aggregation : aggregationList) {
            byte[] bytes = ExpressionCodec.encode(DefaultExpressionCodecContext.ENCODE_CONTEXT, aggregation);
            builder.addAggregation(ByteString.copyFrom(bytes));
        }

        LeoAggregateRequest request = builder.build();
        Batch.Call<LeoAggregateService, List<LeoAggregateResponse.Group>> call = new AggregateCall(request);
        AggregateCallBack callBack = new AggregateCallBack();

        TableName tableName = this.tableReference.getIdentifier().getFullTableName();
        try {
            Table table = executeEngine.getConnection().getTable(tableName);
            table.coprocessorService(LeoAggregateService.class, startRowKey, endRowKey, call, callBack);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Throwable throwable) {
            LOG.error(throwable.getMessage(), throwable);
            throw new InternalException(throwable.getMessage());
        }
    }

    private byte[] getTableReference(Evaluation<Boolean> condition) {
        ByteBuffer buffer = BUFFER_THREAD_LOCAL.get();
        buffer.clear();
        List<Expression> tableExpressions = new ArrayList<>(1);
        if (!this.aggregationList.isEmpty()) {
            tableExpressions.addAll(this.aggregationList);
        }
        if (this.groupByEvaluations != null && !this.groupByEvaluations.isEmpty()) {
            tableExpressions.addAll(this.groupByEvaluations);
        }
        if (condition != null) {
            tableExpressions.add(condition);
        }
        HBaseTableReferenceCodec.encode(this.tableReference, tableExpressions,
            DefaultExpressionCodecContext.ENCODE_CONTEXT, buffer);
        byte[] tableReferenceBytes = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(tableReferenceBytes);
        return tableReferenceBytes;
    }

    private LeoAggregateRequest.Builder getBuilderWithRowKey(byte[] startRowKey, boolean isIncludeStartRowKey,
        byte[] endRowKey, boolean isIncludeEndRowKey) {
        LeoAggregateRequest.Builder builder = LeoAggregateRequest.newBuilder();

        // 根据请求，获得startkey和rowkey，需要考虑是否包含Rowkey本身
        LeoAggregateRequest.RowKey.Builder startRowKeyBuilder = LeoAggregateRequest.RowKey.newBuilder();
        startRowKeyBuilder.setRowKey(ByteString.copyFrom(startRowKey));
        startRowKeyBuilder.setIsInclude(isIncludeStartRowKey);
        builder.setStartRowKey(startRowKeyBuilder);
        LeoAggregateRequest.RowKey.Builder endRowKeyBuilder = LeoAggregateRequest.RowKey.newBuilder();
        endRowKeyBuilder.setRowKey(ByteString.copyFrom(endRowKey));
        endRowKeyBuilder.setIsInclude(isIncludeEndRowKey);
        builder.setStartRowKey(endRowKeyBuilder);

        builder.setStartRowKey(startRowKeyBuilder);
        builder.setEndRowKey(endRowKeyBuilder);
        return builder;
    }

    private byte[] buildPrimaryKey(final IndexPrefixMatchResult primaryKeyMatchResult,
        final SelectPlanner.PrimaryKeyValueIterator primaryKeyValueIterator,
        final ValueRange.IntervalBoundary intervalBoundary) {
        primaryKeyValueIterator.setPrimaryKeyValueList(primaryKeyMatchResult, intervalBoundary);
        return this.primaryKeyCodec.encode(primaryKeyValueIterator);
    }

    /**
     * 此处负责将每个Region中返回的结果更新到Aggrator的Partition中
     * 最终由父Aggregator来负责最终的汇聚结果的汇总
     *
     * @since 2019-06-20
     */
    private class AggregateCallBack implements Batch.Callback<List<LeoAggregateResponse.Group>> {
        @Override
        public void update(byte[] region, byte[] row, List<LeoAggregateResponse.Group> result) {
            if (checkNull(region) || checkNull(row)) {
                LOG.error("Input should not be null.");
                return;
            }

            for (LeoAggregateResponse.Group group : result) {
                List<ByteString> rowList = group.getRow().getResultList();
                for (int i = 0; i < rowList.size(); i++) {
                    ByteString byteString = rowList.get(i);
                    List<Aggregator> aggregatorList = getGroupAggregatorList(group.getGroupKey());
                    Aggregator aggregator = aggregatorList.get(i);
                    Aggregator.Partition partition = ((Aggregator.Partible) aggregator).newPartition(
                        byteString.asReadOnlyByteBuffer());
                    ((Aggregator.Partible) aggregator).aggregate(partition);
                }
            }
        }
    }

    /**
     * 汇聚集合调用，返回单个Region的结果
     *
     * @since 2019-06-20
     */
    private class AggregateCall implements Batch.Call<LeoAggregateService, List<LeoAggregateResponse.Group>> {
        private LeoAggregateRequest request;

        AggregateCall(LeoAggregateRequest request) {
            this.request = request;
        }

        public List<LeoAggregateResponse.Group> call(LeoAggregateService instance) throws IOException {
            ServerRpcController controller = new ServerRpcController();
            CoprocessorRpcUtils.BlockingRpcCallback<LeoAggregateResponse> rpcCallback
                = new CoprocessorRpcUtils.BlockingRpcCallback<>();
            instance.aggregate(controller, request, rpcCallback);

            LeoAggregateResponse response = rpcCallback.get();
            if (controller.failedOnException()) {
                throw controller.getFailedOn();
            }
            return response.getGroupList();
        }
    }
}