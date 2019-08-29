package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLValues extends SyntaxTree.Node {

  public SQLValues(
      @Required final SQLSymbols.L_PARENTHESIS leftParenthesis,
      @Required final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> values,
      @Required final SQLSymbols.R_PARENTHESIS rightParenthesis
      ) {
    if (leftParenthesis == null) {
      throw new IllegalArgumentException("Argument [leftParenthesis] is null.");
    }
    if (values == null) {
      throw new IllegalArgumentException("Argument [values] is null.");
    }
    if (rightParenthesis == null) {
      throw new IllegalArgumentException("Argument [rightParenthesis] is null.");
    }
    this.leftParenthesis = leftParenthesis;
    this.values = values;
    this.rightParenthesis = rightParenthesis;
  }

  private final SQLSymbols.L_PARENTHESIS leftParenthesis;

  private final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> values;

  private final SQLSymbols.R_PARENTHESIS rightParenthesis;

  public SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> getValues() {
    return this.values;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.leftParenthesis.toString(stringBuilder);
    this.values.toString(stringBuilder);
    this.rightParenthesis.toString(stringBuilder);
  }

}
