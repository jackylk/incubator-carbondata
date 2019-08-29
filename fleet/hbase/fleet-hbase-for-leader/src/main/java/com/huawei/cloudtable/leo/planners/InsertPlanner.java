package com.huawei.cloudtable.leo.planners;

import com.huawei.cloudtable.leo.ExecuteContext;
import com.huawei.cloudtable.leo.ExecuteMutations;
import com.huawei.cloudtable.leo.ExecuteParameters;
import com.huawei.cloudtable.leo.ExecutePlan;
import com.huawei.cloudtable.leo.HBaseExecuteEngine;
import com.huawei.cloudtable.leo.HBaseExecuteMutations;
import com.huawei.cloudtable.leo.HBaseExecutePlanner;
import com.huawei.cloudtable.leo.HBaseRowKey;
import com.huawei.cloudtable.leo.HBaseRowKeyCodec;
import com.huawei.cloudtable.leo.HBaseRowKeyValueIterator;
import com.huawei.cloudtable.leo.HBaseRowMutation;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.statements.InsertStatement;
import java.util.Arrays;
import java.util.List;

public abstract class InsertPlanner<TInsertStatement extends InsertStatement>
    extends HBaseExecutePlanner<ExecuteMutations, TInsertStatement> {

  private InsertPlanner(final Class<TInsertStatement> statementClass) {
    super(statementClass);
  }

  public static final class FromValues extends InsertPlanner<InsertStatement.FromValues> {

    public FromValues() {
      super(InsertStatement.FromValues.class);
    }

    @Override
    public InsertPlan plan(
        final HBaseExecuteEngine executeEngine,
        final ExecuteContext executeContext,
        final InsertStatement.FromValues statement
    ) {
      if (!(statement.getTableReference() instanceof HBaseTableReference)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      final HBaseTableReference tableReference = (HBaseTableReference) statement.getTableReference();
      if (!tableReference.getTenantIdentifier().equals(executeContext.getTenantIdentifier())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return new InsertPlan(executeEngine, tableReference, statement);
    }

    private static final class InsertPlan extends ExecutePlan<HBaseExecuteMutations> {

      private static final ThreadLocal<PrimaryKeyValueIterator> PRIMARY_KEY_VALUE_ITERATOR_CACHE;

      private static final ThreadLocal<HBaseRowMutation.Builder> ROW_MUTATION_BUILDER_CACHE;

      static {
        PRIMARY_KEY_VALUE_ITERATOR_CACHE = new ThreadLocal<PrimaryKeyValueIterator>() {
          @Override
          protected PrimaryKeyValueIterator initialValue() {
            return new PrimaryKeyValueIterator();
          }
        };
        ROW_MUTATION_BUILDER_CACHE = new ThreadLocal<HBaseRowMutation.Builder>() {
          @Override
          protected HBaseRowMutation.Builder initialValue() {
            return new HBaseRowMutation.Builder();
          }
        };
      }

      InsertPlan(
          final HBaseExecuteEngine executeEngine,
          final HBaseTableReference tableReference,
          final InsertStatement.FromValues statement
      ) {
        this.executeEngine = executeEngine;
        this.tableReference = tableReference;
        this.statement = statement;
      }

      private final HBaseExecuteEngine executeEngine;

      private final HBaseTableReference tableReference;

      private final InsertStatement.FromValues statement;

      @Override
      public HBaseExecuteMutations execute() {
        final TableDefinition.PrimaryKey primaryKey = this.tableReference.getPrimaryKey();
        if (primaryKey == null) {
          // TODO
          throw new UnsupportedOperationException();
        } else {
          final List<TableDefinition.Column<?>> columnList = this.statement.getColumnList();
          final int[] columnIndexList = new int[columnList.size()];
          final int[] primaryKeyValueIndexList = new int[primaryKey.getColumnCount()];
          Arrays.fill(columnIndexList, -1);
          Arrays.fill(primaryKeyValueIndexList, -1);
          for (int columnIndexInCommand = 0; columnIndexInCommand < columnList.size(); columnIndexInCommand++) {
            final Integer columnIndexInPrimaryKey = primaryKey.getColumnIndex(columnList.get(columnIndexInCommand));
            if (columnIndexInPrimaryKey == null) {
              columnIndexList[columnIndexInCommand] = this.tableReference.getColumnIndex(columnList.get(columnIndexInCommand));
            } else {
              primaryKeyValueIndexList[columnIndexInPrimaryKey] = columnIndexInCommand;
            }
          }
          final HBaseRowKeyCodec primaryKeyCodec = this.tableReference.getPrimaryKeyCodec();
          final List<Evaluation<?>[]> valuesList = this.statement.getValuesList();
          final HBaseExecuteMutations executeMutations = new HBaseExecuteMutations(this.executeEngine, valuesList.size());
          for (int valuesIndex = 0; valuesIndex < valuesList.size(); valuesIndex++) {
            executeMutations.addPut(
                this.tableReference,
                buildRowKey(primaryKeyValueIndexList, primaryKeyCodec, valuesList.get(valuesIndex)),
                buildRowMutation(columnIndexList, valuesList.get(valuesIndex))
            );
          }
          return executeMutations;
        }
      }

      @Override
      public HBaseExecuteMutations execute(final ExecuteParameters parameters) {
        // TODO
        throw new UnsupportedOperationException();
      }

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
      private static HBaseRowMutation buildRowMutation(
          final int[] columnIndexList,
          final Evaluation<?>[] valueList) {
        final HBaseRowMutation.Builder rowMutationBuilder = ROW_MUTATION_BUILDER_CACHE.get();
        try {
          for (int index = 0; index < columnIndexList.length; index++) {
            if (columnIndexList[index] < 0) {
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
            rowMutationBuilder.setColumnValue(columnIndexList[index], value);
          }
          return rowMutationBuilder.build();
        } finally {
          rowMutationBuilder.reset();
        }
      }

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

}
