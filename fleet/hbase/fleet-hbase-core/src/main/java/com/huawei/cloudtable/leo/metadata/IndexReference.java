package com.huawei.cloudtable.leo.metadata;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.expression.Evaluation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexReference {

  public IndexReference(final TableReference tableReference, final IndexDefinition indexDefinition) {
    if (tableReference == null) {
      throw new IllegalArgumentException("Argument [tableReference] is null.");
    }
    if (indexDefinition == null) {
      throw new IllegalArgumentException("Argument [indexDefinition] is null.");
    }
    final List<ColumnReference<?>> indexColumnReferenceList = new ArrayList<>(indexDefinition.getColumnCount());
    for (int index = 0; index < indexDefinition.getColumnCount(); index++) {
      indexColumnReferenceList.add(new ColumnReference<>(indexDefinition.getColumn(index)));
    }
    this.tableReference = tableReference;
    this.indexDefinition = indexDefinition;
    this.indexColumnReferenceList = indexColumnReferenceList;
  }

  private final TableReference tableReference;

  private final IndexDefinition indexDefinition;

  private final List<ColumnReference<?>> indexColumnReferenceList;

  public TableReference getTableReference() {
    return this.tableReference;
  }

  public IndexDefinition getIndexDefinition() {
    return this.indexDefinition;
  }

  public Identifier getName() {
    return this.indexDefinition.getName();
  }

  public IndexDefinition.Type getType() {
    return this.indexDefinition.getType();
  }

  public int getColumnCount() {
    return this.indexDefinition.getColumnCount();
  }

  /**
   * Ordered.
   */
  public List<IndexDefinition.Column<?>> getColumnList() {
    return this.indexDefinition.getColumnList();
  }

  public IndexDefinition.Column<?> getColumn(final int columnIndex) {
    return this.indexDefinition.getColumn(columnIndex);
  }

  public Integer getColumnIndex(final IndexDefinition.Column<?> column) {
    return this.indexDefinition.getColumnIndex(column);
  }

  public ColumnReference<?> getColumnReference(final int columnIndex) {
    return this.indexColumnReferenceList.get(columnIndex);
  }

  public ColumnReference<?> getColumnReference(final IndexDefinition.Column<?> column) {
    final Integer columnIndex = this.getColumnIndex(column);
    if (columnIndex == null) {
      // TODO
      throw new UnsupportedOperationException();
    }
    return this.indexColumnReferenceList.get(columnIndex);
  }

  public String getDescription() {
    return this.indexDefinition.getDescription();
  }

  public Map<Identifier, String> getProperties() {
    return this.indexDefinition.getProperties();
  }

  public String getProperty(final Identifier propertyName) {
    return this.indexDefinition.getProperty(propertyName);
  }

  public static final class ColumnReference<TValue> {

    ColumnReference(final IndexDefinition.Column<TValue> column) {
      this.column = column;
      this.columnExpression = column.getExpression();// TODO 表达式要转换
    }

    private final IndexDefinition.Column<TValue> column;

    private final Evaluation<TValue> columnExpression;

    public IndexDefinition.Column<TValue> getColumn() {
      return this.column;
    }

    public Evaluation<TValue> getColumnExpression() {
      return this.columnExpression;
    }

  }

}
