package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLInExpression extends SQLExpression.Priority3 {

  public SQLInExpression(
      @Required final SQLExpression.Priority3 valueParameter,
      @Required final SQLKeywords.IN in,
      @Required final SQLSymbols.L_PARENTHESIS leftParenthesis,
      @Required final SyntaxTree.NodeList<SQLExpression.Priority3, SQLSymbols.COMMA> parameters,
      @Required final SQLSymbols.R_PARENTHESIS rightParenthesis
      ) {
    if (valueParameter == null) {
      throw new IllegalArgumentException("Argument [valueParameter] is null.");
    }
    if (in == null) {
      throw new IllegalArgumentException("Argument [in] is null.");
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
    this.valueParameter = valueParameter;
    this.in = in;
    this.leftParenthesis = leftParenthesis;
    this.parameters = parameters;
    this.rightParenthesis = rightParenthesis;
  }

  private final SQLExpression valueParameter;

  private final SQLKeywords.IN in;

  private final SQLSymbols.L_PARENTHESIS leftParenthesis;

  private final SyntaxTree.NodeList<SQLExpression.Priority3, SQLSymbols.COMMA> parameters;

  private final SQLSymbols.R_PARENTHESIS rightParenthesis;

  public SQLExpression getValueParameter() {
    return this.valueParameter;
  }

  public SyntaxTree.NodeList<SQLExpression.Priority3, SQLSymbols.COMMA> getParameters() {
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
    this.valueParameter.toString(stringBuilder);
    stringBuilder.append(' ');
    this.in.toString(stringBuilder);
    stringBuilder.append(' ');
    this.leftParenthesis.toString(stringBuilder);
    this.parameters.toString(stringBuilder);
    this.rightParenthesis.toString(stringBuilder);
  }

}
