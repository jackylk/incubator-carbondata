/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.cloudtable.leo.aggregate;

import com.huawei.cloudtable.leo.HBaseRowKeyCodec;
import com.huawei.cloudtable.leo.HBaseRowKeyDefinition;
import com.huawei.cloudtable.leo.HBaseRowKeyValueBytes;
import com.huawei.cloudtable.leo.HBaseTableReferencePart;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Reference;
import com.huawei.cloudtable.leo.expression.Variable;

import org.apache.hadoop.hbase.Cell;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 汇聚计算运行期上下文
 *
 * @author z00337730
 * @since 2019-06-20
 */
final class AggregateRuntimeContext implements Evaluation.RuntimeContext {
    private static final int DEFAULT_COLUMN_LENGTH = 10;

    private final HBaseTableReferencePart tableDefinition;

    private final HBaseRowKeyDefinition rowKeyDefinition;

    private final HBaseRowKeyCodec rowKeyCodec;

    private final Map<Integer, byte[]> currentRow = new HashMap<>(DEFAULT_COLUMN_LENGTH);

    private final HBaseTableReferencePart.ColumnIdentifier columnIdentifier
        = new HBaseTableReferencePart.ColumnIdentifier();

    AggregateRuntimeContext(HBaseTableReferencePart tableDefinition) {
        this.tableDefinition = tableDefinition;
        if (tableDefinition.getPrimaryKey() == null) {
            this.rowKeyDefinition = null;
            this.rowKeyCodec = null;
        } else {
            this.rowKeyDefinition = tableDefinition.getPrimaryKey().getDefinition();
            this.rowKeyCodec = tableDefinition.getPrimaryKey().getCodecType().newCodec(rowKeyDefinition);
        }
    }

    void initCurrentRow(List<Cell> row) {
        this.currentRow.clear();
        if (row.isEmpty()) {
            throw new IllegalArgumentException("Row should not be empty.");
        }

        if (this.rowKeyDefinition != null) {
            initRowKey(row);
        }
        initValue(row);
    }

    private void initValue(List<Cell> row) {
        for (Cell cell : row) {
            this.columnIdentifier.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            final HBaseTableReferencePart.ColumnReference columnReference = this.tableDefinition.getColumn(
                this.columnIdentifier);
            if (columnReference == null) {
                continue;
            }
            this.currentRow.put(columnReference.getIndexInTable(),
                Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(),
                    cell.getValueOffset() + cell.getValueLength()));
        }
    }

    private void initRowKey(List<Cell> row) {
        HBaseRowKeyCodec.Decoder decoder = rowKeyCodec.decoder();
        Cell rowKeyCell = row.get(0);
        decoder.setRowKey(rowKeyCell.getRowArray(), rowKeyCell.getRowOffset(), rowKeyCell.getRowLength());

        HBaseTableReferencePart.ColumnReference primaryKeyColumnReference;
        for (int index = 0; index < rowKeyDefinition.getColumnCount(); index++) {
            decoder.next();
            primaryKeyColumnReference = tableDefinition.getColumn(rowKeyDefinition.getColumnIndexInTable(index));
            if (primaryKeyColumnReference != null) {
                HBaseRowKeyValueBytes rowKeyValueBytes = null;
                ValueBytes valueBytes = decoder.getAsBytes();
                byte[] rowKey = new byte[valueBytes.getLength()];
                byte[] srcBytes = valueBytes.getBytes();
                System.arraycopy(srcBytes,valueBytes.getOffset(), rowKey, 0, rowKey.length);

                this.currentRow.put(primaryKeyColumnReference.getIndexInTable(), rowKey);
            }
        }
    }

    @Override
    public <TValue> TValue getVariable(Variable<TValue> variable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> ValueBytes getVariableAsBytes(Variable<TValue> variable, ValueCodec<TValue> valueCodec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> TValue getAttribute(Reference<TValue> reference) {
        final HBaseTableReferencePart.ColumnReference column = this.tableDefinition.getColumn(
            reference.getAttributeIndex());
        Object value = column.getCodec().decode(this.currentRow.get(reference.getAttributeIndex()));
        return reference.getResultClass().cast(value);
    }

    @Override
    public <TValue> ValueBytes getAttributeAsBytes(Reference<TValue> reference, ValueCodec<TValue> valueCodec) {
        return ValueBytes.of(this.currentRow.get(reference.getAttributeIndex()));
    }
}