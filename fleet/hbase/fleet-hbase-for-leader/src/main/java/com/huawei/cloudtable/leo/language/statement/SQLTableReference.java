package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public abstract class SQLTableReference extends SyntaxTree.Node {

  private SQLTableReference() {
    // to do nothing.
  }

  public static final class Named extends SQLTableReference {

    public Named(
        @Required final SQLTableIdentifier name,
        @Optional final SQLAlias alias
    ) {
      if (name == null) {
        throw new IllegalArgumentException("Argument [name] is null.");
      }
      this.name = name;
      this.alias = alias;
    }

    private final SQLTableIdentifier name;

    private final SQLAlias alias;

    public SQLAlias getAlias() {
      return this.alias;
    }

    public SQLTableIdentifier getFullName() {
      return this.name;
    }

    public SQLIdentifier getSchemaName() {
      return this.name.getSchemaName();
    }

    public SQLIdentifier getTableName() {
      return this.name.getTableName();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.name.toString(stringBuilder);
      if (this.alias != null) {
        stringBuilder.append(' ');
        this.alias.toString(stringBuilder);
      }
    }

  }

}
