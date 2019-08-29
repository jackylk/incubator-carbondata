package com.huawei.cloudtable.leo.metadata;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.ValueCodec;

import java.util.List;
import java.util.Map;

/**
 * 已经在数据库中存在的表，TableReference是与具体的DataEngine强相关的。
 */
public abstract class TableReference {

  public TableReference(
      final String tenantIdentifier,
      final Identifier schemaName,
      final TableDefinition tableDefinition
  ) {
    if (tenantIdentifier == null) {
      throw new IllegalArgumentException("Argument [tenantIdentifier] is null.");
    }
    if (schemaName == null) {
      throw new IllegalArgumentException("Argument [schemaName] is null.");
    }
    if (tableDefinition == null) {
      throw new IllegalArgumentException("Argument [tableDefinition] is null.");
    }
    final Table.SchemaBuilder tableSchemaBuilder = new Table.SchemaBuilder();
    for (int index = 0; index < tableDefinition.getColumnCount(); index++) {
      final TableDefinition.Column<?> column = tableDefinition.getColumn(index);
      tableSchemaBuilder.addAttribute(column.getValueClass(), column.isNullable());
    }
    this.tenantIdentifier = tenantIdentifier;
    this.schemaName = schemaName;
    this.tableDefinition = tableDefinition;
    this.tableSchema = tableSchemaBuilder.build();
  }

  private final String tenantIdentifier;

  private final Identifier schemaName;

  private final TableDefinition tableDefinition;

  private final Table.Schema tableSchema;

  public String getTenantIdentifier() {
    return this.tenantIdentifier;
  }

  public Identifier getSchemaName() {
    return this.schemaName;
  }

  public TableDefinition getTableDefinition() {
    return this.tableDefinition;
  }

  public Table.Schema getTableSchema() {
    return this.tableSchema;
  }

  public Identifier getName() {
    return this.tableDefinition.getName();
  }

  public TableDefinition.PrimaryKey getPrimaryKey() {
    return this.tableDefinition.getPrimaryKey();
  }

  public int getColumnCount() {
    return this.tableDefinition.getColumnCount();
  }

  public List<TableDefinition.Column<?>> getColumnList() {
    return this.tableDefinition.getColumnList();
  }

  public TableDefinition.Column<?> getColumn(final Identifier columnName) {
    return this.tableDefinition.getColumn(columnName);
  }

  public TableDefinition.Column<?> getColumn(final int columnIndex) {
    return this.tableDefinition.getColumn(columnIndex);
  }

  public Integer getColumnIndex(final Identifier columnName) {
    return this.tableDefinition.getColumnIndex(columnName);
  }

  public Integer getColumnIndex(final TableDefinition.Column<?> column) {
    return this.tableDefinition.getColumnIndex(column);
  }

  public abstract ValueCodec<?> getColumnCodec(Identifier columnName);

  public abstract <TValue> ValueCodec<TValue> getColumnCodec(TableDefinition.Column<TValue> column);

  public abstract ValueCodec<?> getColumnCodec(int columnIndex);

  public List<TableDefinition.Constraint> getConstraintList() {
    return this.tableDefinition.getConstraintList();
  }

  public String getDescription() {
    return this.tableDefinition.getDescription();
  }

  public Map<Identifier, String> getProperties() {
    return this.tableDefinition.getProperties();
  }

  public String getProperty(final Identifier propertyName) {
    return this.tableDefinition.getProperty(propertyName);
  }

}
