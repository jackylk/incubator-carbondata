package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.statement.SQLIdentifier;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLFunctionExpression extends SQLExpression.Priority0 {

  public SQLFunctionExpression(
      @Required final SQLIdentifier name,
      @Required final SQLSymbols.L_PARENTHESIS leftParenthesis,
      @Required final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> parameters,
      @Required final SQLSymbols.R_PARENTHESIS rightParenthesis
      ) throws SyntaxException {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    if (leftParenthesis == null) {
      throw new IllegalArgumentException("Argument [leftParenthesis] is null.");
    }
    if (parameters == null) {
      throw new IllegalArgumentException("Argument [parameters] is null.");
    }
    if (rightParenthesis == null) {
      throw new IllegalArgumentException("Argument [rightParenthesis] is null.");
    }
    if (name.haveQuota()) {
      throw new SyntaxException(name.getPosition());
    }
    this.name = name;
    this.leftParenthesis = leftParenthesis;
    this.parameters = parameters;
    this.rightParenthesis = rightParenthesis;
  }

  private final SQLIdentifier name;

  private final SQLSymbols.L_PARENTHESIS leftParenthesis;

  private final SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> parameters;

  private final SQLSymbols.R_PARENTHESIS rightParenthesis;

  public SQLIdentifier getName() {
    return this.name;
  }

  public SyntaxTree.NodeList<SQLExpression, SQLSymbols.COMMA> getParameters() {
    return this.parameters;
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
    this.name.toString(stringBuilder);
    this.leftParenthesis.toString(stringBuilder);
    this.parameters.toString(stringBuilder);
    this.rightParenthesis.toString(stringBuilder);
  }

}
