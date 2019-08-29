package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.Reference;
import com.huawei.cloudtable.leo.metadata.IndexDefinition;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.metadata.TableReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

public final class HBaseTableReference extends TableReference {

  public HBaseTableReference(
      final String tenantIdentifier,
      final HBaseIdentifier schemaName,
      final TableDefinition tableDefinition,
      final List<HBaseTableColumnReference<?>> columnReferenceList,
      final HBaseRowKeyCodecType primaryKeyCodecType,
      final HBaseIdentifier rowMarkColumnFamily,
      final HBaseIdentifier rowMarkColumnQualifier
  ) {
    super(tenantIdentifier, schemaName, tableDefinition);
    final HBaseIdentifier name;
    if (tableDefinition.getName() instanceof HBaseIdentifier) {
      name = (HBaseIdentifier) tableDefinition.getName();
    } else {
      name = HBaseIdentifier.of(tableDefinition.getName().toString());
    }
    if (columnReferenceList.size() != tableDefinition.getColumnCount()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    final TableDefinition.PrimaryKey primaryKey = tableDefinition.getPrimaryKey();
    for (int columnIndex = 0; columnIndex < tableDefinition.getColumnCount(); columnIndex++) {
      final HBaseTableColumnReference<?> columnReference = columnReferenceList.get(columnIndex);
      final TableDefinition.Column<?> column = tableDefinition.getColumn(columnIndex);
      if (columnReference.getColumn() != column) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (primaryKey != null && primaryKey.getColumnIndex(column) != null) {
        if (columnReference.getFamily() != null || columnReference.getQualifier() != null) {
          // TODO
          throw new UnsupportedOperationException();
        }
      } else {
        if (columnReference.getFamily() == null || columnReference.getQualifier() == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
    }
    this.identifier = new HBaseTableIdentifier(tenantIdentifier, schemaName, name);
    this.name = name;
    this.columnReferenceList = columnReferenceList;
    this.primaryKeyCodecType = primaryKeyCodecType;
    this.rowMarkColumnFamily = rowMarkColumnFamily;
    this.rowMarkColumnQualifier = rowMarkColumnQualifier;
  }

  private final HBaseTableIdentifier identifier;

  private final HBaseIdentifier name;

  private final List<HBaseTableColumnReference<?>> columnReferenceList;

  private HBaseIndexReference primaryKeyIndex;

  private HBaseRowKeyDefinition primaryKeyDefinition;

  private final HBaseRowKeyCodecType primaryKeyCodecType;

  private final HBaseIdentifier rowMarkColumnFamily;

  private final HBaseIdentifier rowMarkColumnQualifier;

  public HBaseTableIdentifier getIdentifier() {
    return this.identifier;
  }

  @Override
  public HBaseIdentifier getName() {
    return this.name;
  }

  public HBaseIndexReference getPrimaryKeyIndex() {
    if (this.primaryKeyIndex == null) {
      synchronized (this) {
        if (this.primaryKeyIndex == null) {
          this.primaryKeyIndex = newPrimaryKeyIndex(this);
        }
      }
    }
    return this.primaryKeyIndex;
  }

  public HBaseRowKeyDefinition getPrimaryKeyDefinition() {
    if (this.primaryKeyDefinition == null) {
      synchronized (this) {
        if (this.primaryKeyDefinition == null) {
          this.primaryKeyDefinition = newPrimaryKeyDefinition(this);
        }
      }
    }
    return this.primaryKeyDefinition;
  }

  public HBaseRowKeyCodecType getPrimaryKeyCodecType() {
    return this.primaryKeyCodecType;
  }

  public HBaseRowKeyCodec getPrimaryKeyCodec() {
    return this.primaryKeyCodecType.newCodec(this.getPrimaryKeyDefinition());
  }

  public HBaseTableColumnReference<?> getColumnReference(final int columnIndex) {
    return this.columnReferenceList.get(columnIndex);
  }

  @Override
  public HBaseValueCodec<?> getColumnCodec(final Identifier columnName) {
    final Integer columnIndex = super.getColumnIndex(columnName);
    return columnIndex == null ? null : this.columnReferenceList.get(columnIndex).getCodec();
  }

  @SuppressWarnings("unchecked")
  public <TValue> HBaseValueCodec<TValue> getColumnCodec(final TableDefinition.Column<TValue> column) {
    final Integer columnIndex = super.getColumnIndex(column);
    return columnIndex == null ? null : (HBaseValueCodec<TValue>) this.columnReferenceList.get(columnIndex).getCodec();
  }

  public HBaseValueCodec getColumnCodec(final int columnIndex) {
    return this.columnReferenceList.get(columnIndex).getCodec();
  }

  public void setRowMark(final Get get) {
    get.addColumn(this.rowMarkColumnFamily.getBytes(), this.rowMarkColumnQualifier.getBytes());
  }

  public void setRowMark(final Scan scan) {
    scan.addColumn(this.rowMarkColumnFamily.getBytes(), this.rowMarkColumnQualifier.getBytes());
  }

  public void setRowMark(final Put put) {
    if (!put.has(this.rowMarkColumnFamily.getBytes(), this.rowMarkColumnQualifier.getBytes())) {
      put.addColumn(this.rowMarkColumnFamily.getBytes(), this.rowMarkColumnQualifier.getBytes(), HConstants.EMPTY_BYTE_ARRAY);
    }
  }

  @Override
  public int hashCode() {
    return this.identifier.hashCode();
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object instanceof HBaseTableReference) {
      return this.identifier.equals(((HBaseTableReference) object).identifier);
    }
    return false;
  }

  private static HBaseIndexReference newPrimaryKeyIndex(final HBaseTableReference tableReference) {
    final TableDefinition.PrimaryKey primaryKey = tableReference.getPrimaryKey();
    final IndexDefinition.Builder primaryKeyIndexDefinitionBuilder = new IndexDefinition.Builder(
        primaryKey.getName(),
        IndexDefinition.Type.B_TREE
    );
    for (int index = 0; index < primaryKey.getColumnCount(); index++) {
      final TableDefinition.Column<?> column = primaryKey.getColumn(index);
      primaryKeyIndexDefinitionBuilder.addColumn(new Reference<>(tableReference.getColumnIndex(column), column.getValueClass(), column.isNullable()));
    }
    return new HBaseIndexReference(tableReference, primaryKeyIndexDefinitionBuilder.build());
  }

  private static HBaseRowKeyDefinition newPrimaryKeyDefinition(final HBaseTableReference tableReference) {
    final TableDefinition.PrimaryKey primaryKey = tableReference.getPrimaryKey();
    final int[] primaryKeyColumnIndexesInTable = new int[primaryKey.getColumnCount()];
    for (int index = 0; index < primaryKey.getColumnCount(); index++) {
      primaryKeyColumnIndexesInTable[index] = tableReference.getColumnIndex(primaryKey.getColumn(index));
    }
    return new HBaseRowKeyDefinition() {
      @Override
      public boolean isTheLastColumn(final int columnIndex) {
        return primaryKey.isTheLastColumn(columnIndex);
      }

      @Override
      public boolean isColumnNullable(final int columnIndex) {
        return tableReference.getColumn(primaryKeyColumnIndexesInTable[columnIndex]).isNullable();
      }

      @Override
      public int getColumnCount() {
        return primaryKey.getColumnCount();
      }

      @Override
      public int getColumnIndexInTable(final int columnIndex) {
        return primaryKeyColumnIndexesInTable[columnIndex];
      }

      @Override
      public HBaseValueCodec getColumnCodec(final int columnIndex) {
        return tableReference.getColumnCodec(primaryKeyColumnIndexesInTable[columnIndex]);
      }
    };
  }

}
