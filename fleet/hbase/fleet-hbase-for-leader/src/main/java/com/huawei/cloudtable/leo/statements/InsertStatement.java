package com.huawei.cloudtable.leo.statements;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.expression.Casting;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.expression.Variable;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.metadata.TableReference;
import com.huawei.cloudtable.leo.statement.DataManipulationStatement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TODO 可参照MySQL，提供多种插入模式，如：IGNORE ON DUPLICATE KEY, UPDATE ON DUPLICATE KEY, REPLACE INTO等，HBase默认应该是REPLACE INTO。
 */
public abstract class InsertStatement extends DataManipulationStatement {

  InsertStatement(
      final TableReference tableReference,
      final List<TableDefinition.Column<?>> columnList,
      final DuplicateKeyPolicy duplicateKeyPolicy
  ) {
    if (tableReference == null) {
      throw new IllegalArgumentException("Argument [tableReference] is null.");
    }
    if (columnList.isEmpty()) {
      throw new IllegalArgumentException("Argument [columnList] is empty.");
    }
    final Set<Identifier> columnNameSet = new HashSet<>(columnList.size());
    for (TableDefinition.Column<?> column : columnList) {
      // 检查是否是table中的列
      if (tableReference.getColumnIndex(column) == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      // 检查是否有重名
      if (columnNameSet.contains(column.getName())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      columnNameSet.add(column.getName());
    }
    this.tableReference = tableReference;
    this.columnList = Collections.unmodifiableList(columnList);
    this.duplicateKeyPolicy = duplicateKeyPolicy == null ? DuplicateKeyPolicy.FAIL : duplicateKeyPolicy;
  }

  private final TableReference tableReference;

  private final List<TableDefinition.Column<?>> columnList;

  private final DuplicateKeyPolicy duplicateKeyPolicy;

  public TableReference getTableReference() {
    return this.tableReference;
  }

  public List<TableDefinition.Column<?>> getColumnList() {
    return this.columnList;
  }

  public DuplicateKeyPolicy getDuplicateKeyPolicy() {
    return this.duplicateKeyPolicy;
  }

  public enum DuplicateKeyPolicy {

    REPLACE,

    IGNORE,

    // UPDATE,

    FAIL

  }

  public static final class FromValues extends InsertStatement {

    public FromValues(
        final TableReference tableReference,
        final DuplicateKeyPolicy duplicateKeyPolicy,
        final List<Evaluation<?>[]> valuesList
    ) {
      this(tableReference, tableReference.getColumnList(), duplicateKeyPolicy, valuesList);
    }

    public FromValues(
        final TableReference tableReference,
        final List<TableDefinition.Column<?>> columnList,
        final DuplicateKeyPolicy duplicateKeyPolicy,
        final List<Evaluation<?>[]> valuesList
    ) {
      super(tableReference, columnList, duplicateKeyPolicy);
      if (valuesList.isEmpty()) {
        throw new IllegalArgumentException("Argument [valuesList] is empty.");
      }
      for (Evaluation<?>[] values : valuesList) {
        if (values.length != columnList.size()) {
          // TODO
          throw new UnsupportedOperationException(values.length + ":" + columnList.size());
        }
        for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
          if (values[valueIndex] == null) {
            if (!columnList.get(valueIndex).isNullable()) {
              // TODO
              throw new UnsupportedOperationException();
            } else {
              continue;
            }
          }
          if (values[valueIndex] instanceof Variable) {
            // TODO
            throw new UnsupportedOperationException();
          } else if (values[valueIndex].getResultClass() != columnList.get(valueIndex).getValueClass()) {
            throw new UnsupportedOperationException();
          }
        }
      }
      this.valuesList = Collections.unmodifiableList(valuesList);
    }

    private final List<Evaluation<?>[]> valuesList;

    public List<Evaluation<?>[]> getValuesList() {
      return this.valuesList;
    }

  }

  public static final class FromSubquery extends InsertStatement {

    public FromSubquery(
        final TableReference tableReference,
        final DuplicateKeyPolicy duplicateKeyPolicy,
        final SelectStatement subquery
    ) {
      this(tableReference, tableReference.getColumnList(), duplicateKeyPolicy, subquery);
    }

    public FromSubquery(
        final TableReference tableReference,
        final List<TableDefinition.Column<?>> columnList,
        final DuplicateKeyPolicy duplicateKeyPolicy,
        final SelectStatement subquery
    ) {
      super(tableReference, columnList, duplicateKeyPolicy);
      if (subquery == null) {
        throw new IllegalArgumentException("Argument [subquery] is null.");
      }
      // TODO check subquery
      this.subquery = subquery;
      throw new UnsupportedOperationException();
    }

    private final SelectStatement subquery;

    public SelectStatement getSubquery() {
      return this.subquery;
    }

  }

}
