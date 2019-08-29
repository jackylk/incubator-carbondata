package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLGroupBy extends SyntaxTree.Node {

  public SQLGroupBy(
      @Required final SQLKeywords.GROUP group,
      @Required final SQLKeywords.BY by,
      @Required final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> expressions
  ) {
    if (group == null) {
      throw new IllegalArgumentException("Argument [group] is null.");
    }
    if (by == null) {
      throw new IllegalArgumentException("Argument [by] is null.");
    }
    if (expressions == null) {
      throw new IllegalArgumentException("Argument [expressions] is null.");
    }
    this.group = group;
    this.by = by;
    this.expressions = expressions;
    this.with = null;
    this.type = null;
  }

  public SQLGroupBy(
      @Required final SQLKeywords.GROUP group,
      @Required final SQLKeywords.BY by,
      @Required final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> expressions,
      @Required final SQLKeywords.WITH with,
      @Required final SQLGroupType type
  ) {
    if (group == null) {
      throw new IllegalArgumentException("Argument [group] is null.");
    }
    if (by == null) {
      throw new IllegalArgumentException("Argument [by] is null.");
    }
    if (expressions == null) {
      throw new IllegalArgumentException("Argument [expressions] is null.");
    }
    if (with == null) {
      throw new IllegalArgumentException("Argument [with] is null.");
    }
    if (type == null) {
      throw new IllegalArgumentException("Argument [type] is null.");
    }
    this.group = group;
    this.by = by;
    this.expressions = expressions;
    this.with = with;
    this.type = type;
  }

  private final SQLKeywords.GROUP group;

  private final SQLKeywords.BY by;

  private final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> expressions;

  private final SQLKeywords.WITH with;

  private final SQLGroupType type;

  public SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> getExpressions() {
    return this.expressions;
  }

  public SQLGroupType getType() {
    return this.type;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.group.toString(stringBuilder);
    stringBuilder.append(' ');
    this.by.toString(stringBuilder);
    stringBuilder.append(' ');
    this.expressions.toString(stringBuilder);
    if (this.with != null) {
      stringBuilder.append(' ');
      this.with.toString(stringBuilder);
    }
    if (this.type != null) {
      stringBuilder.append(' ');
      this.type.toString(stringBuilder);
    }
  }

}
