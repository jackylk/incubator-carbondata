package com.huawei.cloudtable.leo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class HBaseExecuteMutations extends ExecuteMutations {

  private static final HBaseRowMutation DELETE_MARK = new HBaseRowMutation.Builder().build();

  public HBaseExecuteMutations(final HBaseExecuteEngine executeEngine) {
    this(executeEngine, 1);
  }

  public HBaseExecuteMutations(final HBaseExecuteEngine executeEngine, final int initialCapacity) {
    if (executeEngine == null) {
      throw new IllegalArgumentException("Argument [executeEngine] is null.");
    }
    this.executeEngine = executeEngine;
    this.mutationsMapByTable = new HashMap<>(initialCapacity);
  }

  protected final HBaseExecuteEngine executeEngine;

  private final Map<HBaseTableReference, Map<HBaseRowKey, HBaseRowMutation>> mutationsMapByTable;

  @Override
  public int getRowCount() {
    int rowCount = 0;
    for (Map mutationsMap : this.mutationsMapByTable.values()) {
      rowCount += mutationsMap.size();
    }
    return rowCount;
  }

  public List<Mutation> getHBaseMutations(final HBaseTableReference tableReference) {
    final Map<HBaseRowKey, HBaseRowMutation> mutations = this.mutationsMapByTable.get(tableReference);
    if (mutations == null) {
      return null;
    } else {
      return toHBaseMutations(tableReference, mutations);
    }
  }

  public void addPut(final HBaseTableReference table, final HBaseRowKey row, final HBaseRowMutation rowMutation) {
    if (rowMutation == null) {
      throw new IllegalArgumentException("Argument [rowMutation] is null.");
    }
    this.getMutations(table).put(row, rowMutation);
  }

  public void addDelete(final HBaseTableReference table, final HBaseRowKey row) {
    this.getMutations(table).put(row, DELETE_MARK);
  }

  @Override
  public void commit() throws ExecuteException {
    for (Map.Entry<HBaseTableReference, Map<HBaseRowKey, HBaseRowMutation>> entry : this.mutationsMapByTable.entrySet()) {
      final HBaseTableReference tableReference = entry.getKey();
      final TableName tableFullName = tableReference.getIdentifier().getFullTableName();
      try {
        final List<Mutation> mutationList = toHBaseMutations(tableReference, entry.getValue());
        // call endpoint
        try (final Table table = this.executeEngine.getConnection().getTable(tableFullName)) {
          try {
            table.batch(mutationList, null);
          } catch (InterruptedException exception) {
            // TODO
            throw new UnsupportedOperationException(exception);
          }
        }
      } catch (IOException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }
  }

  @Override
  public void rollback() {
    this.mutationsMapByTable.clear();
  }

  @Override
  public void join(final ExecuteMutations executeMutations) {
    if (!(executeMutations instanceof HBaseExecuteMutations)) {
      throw new UnsupportedOperationException();
    }
    this.join((HBaseExecuteMutations) executeMutations);
  }

  public void join(final HBaseExecuteMutations executeMutations) {
    for (Map.Entry<HBaseTableReference, Map<HBaseRowKey, HBaseRowMutation>> entry : executeMutations.mutationsMapByTable.entrySet()) {
      final Map<HBaseRowKey, HBaseRowMutation> targetMutations = this.mutationsMapByTable.get(entry.getKey());
      if (targetMutations == null) {
        this.mutationsMapByTable.put(entry.getKey(), entry.getValue());
      } else {
        join(targetMutations, entry.getValue());
      }
    }
  }

  private void join(final Map<HBaseRowKey, HBaseRowMutation> targetMutations, final Map<HBaseRowKey, HBaseRowMutation> sourceMutations) {
    // TODO 要考虑增删改混用的情况
    for (Map.Entry<HBaseRowKey, HBaseRowMutation> entry : sourceMutations.entrySet()) {
      targetMutations.put(entry.getKey(), entry.getValue());
    }
  }

  private Map<HBaseRowKey, HBaseRowMutation> getMutations(final HBaseTableReference table) {
    Map<HBaseRowKey, HBaseRowMutation> mutations = this.mutationsMapByTable.get(table);
    if (mutations == null) {
      mutations = new HashMap<>();
      this.mutationsMapByTable.put(table, mutations);
    }
    return mutations;
  }

  private static List<Mutation> toHBaseMutations(
      final HBaseTableReference tableReference,
      final Map<HBaseRowKey, HBaseRowMutation> mutations
  ) {
    final List<Mutation> mutationList = new ArrayList<>(mutations.size());
    for (Map.Entry<HBaseRowKey, HBaseRowMutation> mutation : mutations.entrySet()) {
      mutationList.add(toHBaseMutation(tableReference, mutation));
    }
    return mutationList;
  }

  @SuppressWarnings("unchecked")
  private static Mutation toHBaseMutation(
      final HBaseTableReference tableReference,
      final Map.Entry<HBaseRowKey, HBaseRowMutation> mutation
  ) {
    if (mutation.getValue() == DELETE_MARK) {
      return mutation.getKey().newDelete();
    } else {
      final Put put = mutation.getKey().newPut();
      final HBaseRowMutation rowMutation = mutation.getValue();
      for (Integer columnIndex : rowMutation.getColumnIndexes()) {
        final HBaseTableColumnReference<?> columnReference = tableReference.getColumnReference(columnIndex);
        final ValueCodec columnCodec = tableReference.getColumnCodec(columnIndex);
        columnReference.setValue(put, columnCodec.encode(rowMutation.getColumnValue(columnIndex)));
      }
      tableReference.setRowMark(put);
      return put;
    }
  }

}
