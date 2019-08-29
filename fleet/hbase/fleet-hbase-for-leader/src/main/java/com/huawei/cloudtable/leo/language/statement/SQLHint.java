package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLHint extends SyntaxTree.Node {

  public SQLHint(@Required final SQLIdentifier name) {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    this.name = name;
  }

  private final SQLIdentifier name;

  public Identifier getName() {
    return this.name.getValue();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.name.toString(stringBuilder);
  }

}
