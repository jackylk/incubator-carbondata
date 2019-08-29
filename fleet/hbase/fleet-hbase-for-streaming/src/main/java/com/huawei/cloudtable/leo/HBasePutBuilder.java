package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import org.apache.hadoop.hbase.client.Put;

import java.util.Arrays;

public final class HBasePutBuilder {

  /**
   * @param tableReference
   * @param columns
   * @param columnValues   长度必须与columns相同
   * @return
   */
  public static Put build(
      final HBaseTableReference tableReference,
      final TableDefinition.Column<?>[] columns,
      final Evaluation<?>[] columnValues
  ) {
    final TableDefinition.PrimaryKey primaryKey = tableReference.getPrimaryKey();
    final int[] columnIndexList = new int[columns.length];
    final int[] primaryKeyValueIndexList = new int[primaryKey.getColumnCount()];
    Arrays.fill(columnIndexList, -1);
    Arrays.fill(primaryKeyValueIndexList, -1);
    for (int columnIndexInCommand = 0; columnIndexInCommand < columns.length; columnIndexInCommand++) {
      final Integer columnIndexInPrimaryKey = primaryKey.getColumnIndex(columns[columnIndexInCommand]);
      if (columnIndexInPrimaryKey == null) {
        columnIndexList[columnIndexInCommand] = tableReference.getColumnIndex(columns[columnIndexInCommand]);
      } else {
        primaryKeyValueIndexList[columnIndexInPrimaryKey] = columnIndexInCommand;
      }
    }
    final HBaseRowKeyCodec primaryKeyCodec = tableReference.getPrimaryKeyCodec();
    final HBaseRowKey rowKey = buildRowKey(primaryKeyValueIndexList, primaryKeyCodec, columnValues);
    final Put put = rowKey.newPut();
    buildRowMutation(tableReference, put, columnIndexList, columnValues);
    tableReference.setRowMark(put);
    return put;
  }

  private static final ThreadLocal<PrimaryKeyValueIterator> PRIMARY_KEY_VALUE_ITERATOR_CACHE = new ThreadLocal<PrimaryKeyValueIterator>() {
    @Override
    protected PrimaryKeyValueIterator initialValue() {
      return new PrimaryKeyValueIterator();
    }
  };

  private static HBaseRowKey buildRowKey(
      final int[] primaryKeyColumnValueIndexList,
      final HBaseRowKeyCodec primaryKeyCodec,
      final Evaluation<?>[] valueList
  ) {
    final PrimaryKeyValueIterator primaryKeyValueIterator = PRIMARY_KEY_VALUE_ITERATOR_CACHE.get();
    primaryKeyValueIterator.setPrimaryKeyValueList(valueList, primaryKeyColumnValueIndexList);
    return HBaseRowKey.of(primaryKeyCodec.encode(primaryKeyValueIterator));
  }

  @SuppressWarnings("unchecked")
  private static void buildRowMutation(
      final HBaseTableReference tableReference,
      final Put put,
      final int[] columnIndexList,
      final Evaluation<?>[] valueList) {
    for (int index = 0; index < columnIndexList.length; index++) {
      final int columnIndex = columnIndexList[index];
      if (columnIndex < 0) {
        continue;
      }
      if (valueList[index] == null) {
        continue;
      }
      final Object value;
      try {
        value = valueList[index].evaluate(null);
      } catch (EvaluateException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
      if (value == null) {
        continue;
      }
      final HBaseTableColumnReference<?> columnReference = tableReference.getColumnReference(columnIndex);
      final ValueCodec columnCodec = tableReference.getColumnCodec(columnIndex);
      columnReference.setValue(put, columnCodec.encode(value));
    }
  }

  private HBasePutBuilder() {
    // to do nothing.
  }

  private static final class PrimaryKeyValueIterator extends HBaseRowKeyValueIterator {

    PrimaryKeyValueIterator() {
      // to do nothing.
    }

    private Evaluation<?>[] valueList;

    private int[] primaryKeyColumnValueIndexList;

    private int currentColumnIndex = -1;

    void setPrimaryKeyValueList(final Evaluation<?>[] primaryKeyColumnValueList, final int[] primaryKeyColumnValueIndexList) {
      this.valueList = primaryKeyColumnValueList;
      this.primaryKeyColumnValueIndexList = primaryKeyColumnValueIndexList;
      this.currentColumnIndex = -1;
    }

    @Override
    public boolean hasNext() {
      return this.currentColumnIndex < this.primaryKeyColumnValueIndexList.length - 1;
    }

    @Override
    public void next() {
      if (this.hasNext()) {
        this.currentColumnIndex++;
      } else {
        throw new RuntimeException("No more data.");
      }
    }

    @Override
    public Object get() {
      if (this.currentColumnIndex < 0) {
        throw new RuntimeException();
      }
      final Evaluation<?> value = this.valueList[this.primaryKeyColumnValueIndexList[this.currentColumnIndex]];
      if (value == null) {
        return null;
      }
      try {
        return value.evaluate(null);// TODO
      } catch (EvaluateException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }

    @Override
    public ValueBytes getAsBytes() {
      throw new UnsupportedOperationException();
    }

  }

}
