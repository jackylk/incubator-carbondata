package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLPreferentialExpression extends SQLExpression.Priority0 {

  public SQLPreferentialExpression(
      @Required final SQLSymbols.L_PARENTHESIS leftParenthesis,
      @Required final SQLExpression expression,
      @Required final SQLSymbols.R_PARENTHESIS rightParenthesis
      ) throws SyntaxException {
    if (leftParenthesis == null) {
      throw new IllegalArgumentException("Argument [leftParenthesis] is null.");
    }
    if (expression == null) {
      throw new IllegalArgumentException("Argument [expression] is null.");
    }
    if (rightParenthesis == null) {
      throw new IllegalArgumentException("Argument [rightParenthesis] is null.");
    }
    this.leftParenthesis = leftParenthesis;
    this.expression = expression;
    this.rightParenthesis = rightParenthesis;
  }

  private final SQLSymbols.L_PARENTHESIS leftParenthesis;

  private final SQLExpression expression;

  private final SQLSymbols.R_PARENTHESIS rightParenthesis;

  public SQLExpression getExpression() {
    return this.expression;
  }

  @Override
  public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
      final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
      final TVisitorParameter visitorParameter
  ) {
    return visitor.visit(this, visitorParameter);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.leftParenthesis.toString(stringBuilder);
    stringBuilder.append(' ');
    this.expression.toString(stringBuilder);
    stringBuilder.append(' ');
    this.rightParenthesis.toString(stringBuilder);
  }

}
