package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLHaving extends SyntaxTree.Node {

  public SQLHaving(
      @Required final SQLKeywords.HAVING having,
      @Required final SQLExpression condition
  ) {
    if (having == null) {
      throw new IllegalArgumentException("Argument [having] is null.");
    }
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    this.having = having;
    this.condition = condition;
  }

  private final SQLKeywords.HAVING having;

  private final SQLExpression condition;

  public SQLExpression getCondition() {
    return this.condition;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.having.toString(stringBuilder);
    stringBuilder.append(' ');
    this.condition.toString(stringBuilder);
  }

}
