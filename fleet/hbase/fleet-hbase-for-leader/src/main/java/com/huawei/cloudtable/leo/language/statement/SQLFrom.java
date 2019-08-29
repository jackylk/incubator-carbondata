package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLFrom extends SyntaxTree.Node {

  public SQLFrom(
      @Required final SQLKeywords.FROM from,
      @Required final SQLTableReference tableReference
  ) {
    if (from == null) {
      throw new IllegalArgumentException("Argument [from] is null.");
    }
    if (tableReference == null) {
      throw new IllegalArgumentException("Argument [tableReference] is null.");
    }
    this.from = from;
    this.tableReference = tableReference;
  }

  private final SQLKeywords.FROM from;

  private final SQLTableReference tableReference;

  public SQLTableReference getTableReference() {
    return this.tableReference;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.from.toString(stringBuilder);
    stringBuilder.append(' ');
    this.tableReference.toString(stringBuilder);
  }

}
