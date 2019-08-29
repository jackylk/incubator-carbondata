package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLWhere extends SyntaxTree.Node {

  public SQLWhere(
      @Required final SQLKeywords.WHERE where,
      @Required final SQLExpression condition
  ) {
    if (where == null) {
      throw new IllegalArgumentException("Argument [where] is null.");
    }
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    this.where = where;
    this.condition = condition;
  }

  private final SQLKeywords.WHERE where;

  private final SQLExpression condition;

  public SQLExpression getCondition() {
    return this.condition;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.where.toString(stringBuilder);
    stringBuilder.append(' ');
    this.condition.toString(stringBuilder);
  }

}
