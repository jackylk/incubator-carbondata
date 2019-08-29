/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.aggregate;

import static com.huawei.cloudtable.leo.ExpressionCodec.DecodeContext;
import static com.huawei.cloudtable.leo.ExpressionCodec.decode;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.HBaseTableReferenceCodec;
import com.huawei.cloudtable.leo.HBaseTableReferencePart;
import com.huawei.cloudtable.leo.HBaseValueCodec;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.aggregate.LeoAggregateProtos.LeoAggregateResponse;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.Aggregator;
import com.huawei.cloudtable.leo.expression.Aggregator.Partible;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.hbase.filters.ConditionFilter;
import com.huawei.cloudtable.leo.value.Timestamp;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javafx.util.Pair;

/**
 * RegionServer中的Endpoint Coprocessor实现，主要负责做数据的汇聚，支持如下类型的语法
 * Select max(c1) from table1 where rowkey> startRowkey and rowKey < endRowkey
 * Select max(c1), avg(c1+c2), min(c3) from table1 where rowkey> startRowkey and rowKey < endRowkey
 * <p>
 * 客户的场景语法为：
 * select sum(item1) from table1 where ts> ts1 and ts < ts2 and dm=xxx group by ts
 *
 * @author z00337730
 * @since 2019-06-20
 */
public class LeoAggregateEndPoint extends LeoAggregateProtos.LeoAggregateService implements RegionCoprocessor {
    private static final Logger LOG = LoggerFactory.getLogger(LeoAggregateEndPoint.class);

    private static final byte[] EMPTY_ROWKEY = new byte[0];

    private static final int DEFAULT_BUFFER_SIZE = 10240;

    private static final int DEFAULT_ROW_SIZE = 16;

    private static final int DEFAULT_GROUP_SIZE = 1;

    private static final byte[] EMPTY_FILTER = new byte[0];

    private static final ThreadLocal<ByteBuffer> BUFFER_THREAD_LOCAL = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        }
    };

    private RegionCoprocessorEnvironment env;

    private DecodeContext expressionDecodeContext = DefaultExpressionCodecContext.DECODE_CONTEXT;

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }

    @Override
    public void aggregate(RpcController controller, LeoAggregateProtos.LeoAggregateRequest request,
        RpcCallback<LeoAggregateProtos.LeoAggregateResponse> done) {
        Map<List<Object>, Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>>> partitionResultMap
            = new HashMap<>(DEFAULT_GROUP_SIZE);

        LOG.info("Decode aggregation request.");
        // 将Table表描述反编译出来，主要编译出来表描述的part部分，需要获取到列名
        ByteBuffer byteBuffer = ByteBuffer.wrap(request.getTableDefine().toByteArray());
        byteBuffer.position(0);
        final HBaseTableReferencePart part = HBaseTableReferenceCodec.decode(byteBuffer, expressionDecodeContext,
            HBaseValueCodecManager.BUILD_IN);

        // 反编译汇聚算法列表，一条SQL语句中，可能会同时存在多个汇聚命令
        List<Aggregation<?>> aggregationList = buildAggregations(controller, request);
        if (aggregationList == null) {
            LOG.error("Aggregation should not be empty.");
            return;
        }
        // need to compile the aggregation's parameter
        for (Aggregation<?> aggregation : aggregationList) {
            for (int i = 0; i < aggregation.getParameterCount(); i++) {
                aggregation.getParameter(i).compile(AggregateCompileContext.COMPILE_CONTEXT);
            }
        }

        final AggregateRuntimeContext runtimeContext = new AggregateRuntimeContext(part);
        LeoAggregateResponse.Builder responseBuilder = LeoAggregateResponse.newBuilder();
        LeoAggregateResponse.Group.Builder groupBuilder = LeoAggregateResponse.Group.newBuilder();
        // 封装Scan对象，查询结果，请求体中要求startRowKey和endRowKey非空
        Scan scan = buildScan(request);
        // 封装GroupBy对象
        Evaluation<?>[] groupByEvaluations = getGroupByEvaluations(request);
        // need to compile the evaluations
        for (Evaluation<?> evaluation : groupByEvaluations) {
            evaluation.compile(AggregateCompileContext.COMPILE_CONTEXT);
        }
        LOG.info("Finished decode the aggregation request.");

        InternalScanner scanner = null;
        // TODO 待补充SCAN按列查询的能力，当前的Part接口中，无法获取到具体的列名
        // 如果TableDefine的Part部分无法返回，则考虑通过接口直接传递
        try {
            scanner = env.getRegion().getScanner(scan);
            boolean hasMore;
            List<Cell> currentRow = new ArrayList<>(DEFAULT_ROW_SIZE);
            int count = 0;
            do {
                hasMore = scanner.next(currentRow);
                if (currentRow.isEmpty()) {
                    LOG.info("Do not find any rows to aggregate.");
                    done.run(responseBuilder.build());
                    return;
                }
                runtimeContext.initCurrentRow(currentRow);

                // 获得GroupByKey
                List<Object> groupByKeyList = new ArrayList<>(groupByEvaluations.length);
                for (Evaluation evaluation : groupByEvaluations) {
                    groupByKeyList.add(evaluation.evaluate(runtimeContext));
                }

                LOG.debug("Build aggregator list and AggregateParameterIterator list.");
                // 封装Aggregator列表 和 AggregateParameterIterator列表
                // 由于存在多个Groupby的场景，每个Groupby，就需要的结果，是通过一组Aggregator来记录
                Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>> pair = getAggregatorPair(
                    partitionResultMap, part, aggregationList, runtimeContext, groupByKeyList);

                // 按条件计算一行记录的聚合结果，结果保存在Aggregator.partition中
                // 最终上报给客户的时候，需要将Aggregator.partition中的值，按groupby id返回
                List<Aggregator.Partition> aggregatorList = pair.getKey();
                List<AggregateParameterIterator> aggregateParameterIteratorList = pair.getValue();
                for (int i = 0; i < aggregatorList.size(); i++) {
                    Aggregator.Partition partition = aggregatorList.get(i);
                    AggregateParameterIterator aggregateParameterIterator = aggregateParameterIteratorList.get(i);
                    partition.aggregate(aggregateParameterIterator);
                    aggregateParameterIterator.reset();
                }

                currentRow.clear();
                count++;
            } while (hasMore);
            LOG.info("Aggregate successful, total aggregate line is: {}", count);

            // 构造结果，一个GroupBykey的结果，就是一行记录，每行记录中，保存多个聚合结果
            buildResponse(partitionResultMap, responseBuilder, groupBuilder);
            done.run(responseBuilder.build());
        } catch (IOException e) {
            CoprocessorRpcUtils.setControllerException(controller, e);
        } catch (EvaluateException ee) {
            LOG.error(ee.getMessage());
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }
    }

    private Scan buildScan(LeoAggregateProtos.LeoAggregateRequest request) {
        // 获取StartRowKey和EndRowKey
        byte[] startRowKey = request.getStartRowKey().getRowKey().toByteArray();
        byte[] endRowKey = request.getEndRowKey().getRowKey().toByteArray();

        Scan scan = new Scan();
        if (!startRowKey.equals(EMPTY_ROWKEY)) {
            boolean isInclude = request.getStartRowKey().getIsInclude();
            scan.withStartRow(startRowKey, isInclude);
        }

        if (!endRowKey.equals(EMPTY_ROWKEY)) {
            boolean isInclude = request.getEndRowKey().getIsInclude();
            scan.withStopRow(endRowKey, isInclude);
        }

        // 封装Filter对象，只有过滤条件为true的，才进入到后继环节
        if ((request.getFilter() != null) && (request.getFilter().toByteArray().equals(EMPTY_FILTER))) {
            Filter filter = ConditionFilter.parseFrom(request.getFilter().toByteArray());
            scan.setFilter(filter);
        }
        return scan;
    }

    private Evaluation<?>[] getGroupByEvaluations(LeoAggregateProtos.LeoAggregateRequest request) {
        Evaluation<?>[] groupByEvaluations = new Evaluation[request.getGroupby().getExpressionCount()];

        List<ByteString> groupByList = request.getGroupby().getExpressionList();
        for (int groupByIndex = 0; groupByIndex < groupByList.size(); groupByIndex++) {
            groupByEvaluations[groupByIndex] = (Evaluation<?>) DefaultExpressionCodecContext.decode(
                groupByList.get(groupByIndex).toByteArray());
        }
        return groupByEvaluations;
    }

    private Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>> getAggregatorPair(
        Map<List<Object>, Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>>> partitionResultMap,
        HBaseTableReferencePart part, List<Aggregation<?>> aggregationList, AggregateRuntimeContext runtimeContext,
        List<Object> groupByKeyList) {
        Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>> pair;
        if (partitionResultMap.get(groupByKeyList) == null) {
            pair = buildGroupAggregatorList(aggregationList, part, runtimeContext);
            partitionResultMap.put(groupByKeyList, pair);
        } else {
            pair = partitionResultMap.get(groupByKeyList);
        }
        return pair;
    }

    /**
     * 将封装结果封装到Response消息体中返回
     * 每个GroupKey返回一行
     *
     * @param partitionResultMap
     * @param responseBuilder
     * @param groupBuilder
     */
    private void buildResponse(
        Map<List<Object>, Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>>> partitionResultMap,
        LeoAggregateResponse.Builder responseBuilder, LeoAggregateResponse.Group.Builder groupBuilder) {
        for (List<Object> groupKey : partitionResultMap.keySet()) {
            List<Aggregator.Partition> partitionList = partitionResultMap.get(groupKey).getKey();
            LeoAggregateResponse.Row.Builder rowBuilder = LeoAggregateResponse.Row.newBuilder();
            for (Aggregator.Partition partition : partitionList) {
                ByteBuffer buffer = BUFFER_THREAD_LOCAL.get();
                buffer.clear();
                partition.serialize(buffer);
                byte[] decodedPartition = new byte[buffer.position()];
                buffer.position(0);
                buffer.get(decodedPartition);
                // 根据HBase内置编解码器，将结果转换成ByteString传递给调用方
                rowBuilder.addResult(ByteString.copyFrom(decodedPartition));
            }

            ByteBuffer buffer = BUFFER_THREAD_LOCAL.get();
            buffer.clear();
            for (Object object : groupKey) {
                HBaseValueCodec valueCodec = HBaseValueCodecManager.BUILD_IN.getValueCodec(object.getClass());
                valueCodec.encode(object, buffer);
            }
            byte[] groupKeyBytes = new byte[buffer.position()];
            LOG.info("GroupKey = {}", groupKeyBytes);
            buffer.position(0);
            buffer.get(groupKeyBytes);
            groupBuilder.setGroupKey(ByteString.copyFrom(groupKeyBytes));
            groupBuilder.setRow(rowBuilder.build());
            responseBuilder.addGroup(groupBuilder.build());
        }
    }

    private Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>> buildGroupAggregatorList(
        List<Aggregation<?>> aggregationList, HBaseTableReferencePart part, AggregateRuntimeContext runtimeContext) {
        List<Aggregator.Partition> aggregatorList = new ArrayList<>(aggregationList.size());
        List<AggregateParameterIterator> aggregateParameterIteratorList = new ArrayList<>(aggregationList.size());
        for (Aggregation aggregation : aggregationList) {
            Partible partible = (Partible) aggregation.newAggregator(AggregateCompileContext.COMPILE_CONTEXT);
            aggregatorList.add(partible.newPartition());
            AggregateParameterIterator aggregateParameterIterator = new AggregateParameterIterator(part, aggregation,
                runtimeContext);
            aggregateParameterIteratorList.add(aggregateParameterIterator);
        }

        Pair<List<Aggregator.Partition>, List<AggregateParameterIterator>> pair = new Pair(aggregatorList,
            aggregateParameterIteratorList);
        LOG.info("Build aggregator size= {}", aggregationList.size());
        return pair;
    }

    private List<Aggregation<?>> buildAggregations(RpcController controller,
        LeoAggregateProtos.LeoAggregateRequest request) {
        List<Aggregation<?>> aggregationList = new ArrayList<>(request.getAggregationCount());
        for (ByteString byteString : request.getAggregationList()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteString.toByteArray());
            Expression expression = decode(expressionDecodeContext, byteBuffer);
            if (expression instanceof Aggregation) {
                aggregationList.add((Aggregation) expression);
            } else {
                LOG.error("Input Parameter Error.Expected Aggregation, but Expression.");
                CoprocessorRpcUtils.setControllerException(controller,
                    new IllegalArgumentIOException("Need Aggregation function."));
                return null;
            }
        }
        return aggregationList;
    }

    @Override
    public void start(CoprocessorEnvironment environment) throws CoprocessorException {
        if (environment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) environment;
        } else {
            throw new CoprocessorException("Must be loaded on a table region.");
        }
    }
}
