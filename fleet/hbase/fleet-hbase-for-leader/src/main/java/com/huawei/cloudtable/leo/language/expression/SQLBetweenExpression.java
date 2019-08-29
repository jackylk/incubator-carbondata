package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLBetweenExpression extends SQLExpression.Priority4 {

  public SQLBetweenExpression(
      @Required final SQLExpression.Priority4 valueParameter,
      @Required final SQLKeywords.BETWEEN between,
      @Required final SQLExpression.Priority4 parameter1,
      @Required final SQLKeywords.AND and,
      @Required final SQLExpression.Priority4 parameter2
      ) {
    if (valueParameter == null) {
      throw new IllegalArgumentException("Argument [valueParameter] is null.");
    }
    if (between == null) {
      throw new IllegalArgumentException("Argument [between] is null.");
    }
    if (parameter1 == null) {
      throw new IllegalArgumentException("Argument [parameter1] is null.");
    }
    if (and == null) {
      throw new IllegalArgumentException("Argument [and] is null.");
    }
    if (parameter2 == null) {
      throw new IllegalArgumentException("Argument [parameter2] is null.");
    }
    this.valueParameter = valueParameter;
    this.between = between;
    this.parameter1 = parameter1;
    this.and = and;
    this.parameter2 = parameter2;
  }

  private final SQLExpression valueParameter;

  private final SQLKeywords.BETWEEN between;

  private final SQLExpression parameter1;

  private final SQLKeywords.AND and;

  private final SQLExpression parameter2;

  public SQLExpression getValueParameter() {
    return this.valueParameter;
  }

  public SQLExpression getParameter1() {
    return this.parameter1;
  }

  public SQLExpression getParameter2() {
    return this.parameter2;
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
    this.between.toString(stringBuilder);
    stringBuilder.append(' ');
    this.parameter1.toString(stringBuilder);
    stringBuilder.append(' ');
    this.and.toString(stringBuilder);
    stringBuilder.append(' ');
    this.parameter2.toString(stringBuilder);
  }

}
