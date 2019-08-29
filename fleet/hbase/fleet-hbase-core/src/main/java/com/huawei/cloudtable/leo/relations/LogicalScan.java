package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.expression.Reference;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.metadata.TableReference;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public final class LogicalScan extends Relation implements Relation.Logical {

  private static final ThreadLocal<Table.SchemaBuilder> SCHEMA_BUILDER_CACHE = new ThreadLocal<Table.SchemaBuilder>(){
    @Override
    protected Table.SchemaBuilder initialValue() {
      return new Table.SchemaBuilder();
    }
  };

  public LogicalScan(final TableReference tableReference, final List<Reference<?>> tableColumnReferenceList
   ) {
    super(buildResultSchema(tableReference, tableColumnReferenceList));
    this.tableReference = tableReference;
    this.tableColumnReferenceList = Collections.unmodifiableList(tableColumnReferenceList);
    this.hashCode = 0;
  }

  private final TableReference tableReference;

  private final List<Reference<?>> tableColumnReferenceList;

  private volatile int hashCode;

  @Override
  public int getInputCount() {
    return 0;
  }

  @Nonnull
  @Override
  public <TRelation extends Relation> TRelation getInput(final int inputIndex) {
    throw new IndexOutOfBoundsException();
  }

  public TableReference getTableReference() {
    return this.tableReference;
  }

  public List<Reference<?>> getTableColumnReferenceList() {
    return this.tableColumnReferenceList;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      int hashCode = 0;
      if (this.tableReference.getSchemaName() != null) {
        hashCode = 31 * hashCode + this.tableReference.getSchemaName().hashCode();
      }
      this.hashCode = 31 * hashCode + this.tableReference.getName().hashCode();
      for (Reference<?> columnReference : this.tableColumnReferenceList) {
        hashCode = 31 * hashCode + columnReference.hashCode();
      }
    }
    return this.hashCode;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (object.getClass() == this.getClass()) {
      final LogicalScan that = (LogicalScan) object;
      return this.tableReference.getName().equals(that.tableReference.getName())
          && this.tableColumnReferenceList.equals(that.tableColumnReferenceList)
          && (this.tableReference.getSchemaName() == null ? that.tableReference.getSchemaName() == null : this.tableReference.getSchemaName().equals(that.tableReference.getSchemaName()));
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("SCAN(");
    if (this.tableReference.getSchemaName() != null) {
      stringBuilder.append("schema=").append(this.tableReference.getSchemaName()).append("; ");
    }
    stringBuilder.append("table=").append(this.tableReference.getName()).append(")");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected final Relation copy(final Relation[] newInputList) {
    return this;
  }

  private static Table.Schema buildResultSchema(final TableReference tableReference, final List<Reference<?>> tableColumnReferenceList) {
    boolean tableColumnReferenceOrdered = true;
    for (int index = 0; index < tableColumnReferenceList.size(); index++) {
      final Reference<?> tableColumnReference = tableColumnReferenceList.get(index);
      final TableDefinition.Column<?> tableColumn = tableReference.getColumn(tableColumnReference.getAttributeIndex());
      if (tableColumn == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (tableColumnReference.getResultClass() != tableColumn.getValueClass()) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (tableColumnReference.isNullable() != tableColumn.isNullable()) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (tableColumnReference.getAttributeIndex() != index) {
        tableColumnReferenceOrdered = false;
      }
    }
    if (tableColumnReferenceOrdered && tableColumnReferenceList.size() == tableReference.getColumnCount()) {
      return tableReference.getTableSchema();
    }
    final Table.SchemaBuilder schemaBuilder = SCHEMA_BUILDER_CACHE.get();
    schemaBuilder.reset();
    for (Reference<?> tableColumnReference : tableColumnReferenceList) {
      schemaBuilder.addAttribute(tableColumnReference.getResultClass(), tableColumnReference.isNullable());
    }
    return schemaBuilder.build();
  }

}
