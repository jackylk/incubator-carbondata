package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLTableIdentifier extends SyntaxTree.Node {

  public SQLTableIdentifier(@Required final SQLIdentifier tableName) {
    if (tableName == null) {
      throw new IllegalArgumentException("Argument [tableName] is null.");
    }
    this.schemaName = null;
    this.point = null;
    this.tableName = tableName;
  }

  public SQLTableIdentifier(
      @Required final SQLIdentifier schemaName,
      @Required final SQLSymbols.POINT point,
      @Required final SQLIdentifier tableName
  ) {
    if (schemaName == null) {
      throw new IllegalArgumentException("Argument [schemaName] is null.");
    }
    if (point == null) {
      throw new IllegalArgumentException("Argument [point] is null.");
    }
    if (tableName == null) {
      throw new IllegalArgumentException("Argument [tableName] is null.");
    }
    this.schemaName = schemaName;
    this.point = point;
    this.tableName = tableName;
  }

  private final SQLIdentifier schemaName;

  private final SQLSymbols.POINT point;

  private final SQLIdentifier tableName;

  public SQLIdentifier getSchemaName() {
    return this.schemaName;
  }

  public SQLIdentifier getTableName() {
    return this.tableName;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    if (this.schemaName != null) {
      if (this.point == null) {
        throw new RuntimeException();
      }
      this.schemaName.toString(stringBuilder);
      this.point.toString(stringBuilder);
    }
    this.tableName.toString(stringBuilder);
  }

}
