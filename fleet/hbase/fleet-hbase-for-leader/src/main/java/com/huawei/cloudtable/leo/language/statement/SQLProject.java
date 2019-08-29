package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLProject extends SyntaxTree.Node {

  public SQLProject(
      @Required final SQLExpression expression,
      @Optional final SQLAlias alias) {
    if (expression == null) {
      throw new IllegalArgumentException("Argument [expression] is null.");
    }
    this.expression = expression;
    this.alias = alias;
  }

  private final SQLExpression expression;

  private final SQLAlias alias;

  public SQLExpression getExpression() {
    return this.expression;
  }

  public SQLAlias getAlias() {
    return this.alias;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.expression.toString(stringBuilder);
    if (this.alias != null) {
      stringBuilder.append(' ');
      this.alias.toString(stringBuilder);
    }
  }

}
