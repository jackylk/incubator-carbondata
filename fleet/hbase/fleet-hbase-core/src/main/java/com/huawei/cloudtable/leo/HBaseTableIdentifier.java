package com.huawei.cloudtable.leo;

import org.apache.hadoop.hbase.TableName;

public final class HBaseTableIdentifier {

  public HBaseTableIdentifier(
      final String tenantIdentifier,
      final HBaseIdentifier schemaName,
      final HBaseIdentifier tableName
  ) {
    if (tableName == null) {
      throw new IllegalArgumentException("Argument [tableName] is null.");
    }
    this.tenantIdentifier = tenantIdentifier;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.hashCode = 0;
    this.fullTableName = buildFullTableName(schemaName, tableName);
  }

  public static TableName buildFullTableName(
      final Identifier schemaName,
      final Identifier tableName
  ) {
    return TableName.valueOf(schemaName.toString(), tableName.toString());
  }

  private final String tenantIdentifier;

  private final HBaseIdentifier schemaName;

  private final HBaseIdentifier tableName;

  private final TableName fullTableName;

  private int hashCode;

  public String getTenantIdentifier() {
    return this.tenantIdentifier;
  }

  public HBaseIdentifier getSchemaName() {
    return this.schemaName;
  }

  public HBaseIdentifier getTableName() {
    return this.tableName;
  }

  public TableName getFullTableName() {
    return this.fullTableName;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      int hashCode = this.tableName.hashCode();
      if (this.tenantIdentifier != null) {
        hashCode = 31 * hashCode + this.tenantIdentifier.hashCode();
      }
      if (this.schemaName != null) {
        hashCode = 31 * hashCode + this.schemaName.hashCode();
      }
      this.hashCode = hashCode;
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
      final HBaseTableIdentifier that = (HBaseTableIdentifier) object;
      return this.tableName.equals(that.tableName)
          && (this.schemaName == null ? that.schemaName == null : this.schemaName.equals(that.schemaName))
          && (this.tenantIdentifier == null ? that.tenantIdentifier == null : this.tenantIdentifier.equals(that.tenantIdentifier));
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    if (this.schemaName == null) {
      return this.tableName.toString();
    } else {
      return this.schemaName.toString() + "." + this.tableName.toString();
    }
  }

}
