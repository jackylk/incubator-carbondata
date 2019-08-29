package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLGreaterExpression extends SQLExpression.Priority3 {

  public SQLGreaterExpression(
      @Required final Priority3 parameter1,
      @Required final SQLSymbols.R_ANGLE_BRACKET greater,
      @Required final Priority3 parameter2
      ) {
    if (parameter1 == null) {
      throw new IllegalArgumentException("Argument [parameter1] is null.");
    }
    if (greater == null) {
      throw new IllegalArgumentException("Argument [greater] is null.");
    }
    if (parameter2 == null) {
      throw new IllegalArgumentException("Argument [parameter2] is null.");
    }
    this.parameter1 = parameter1;
    this.greater = greater;
    this.parameter2 = parameter2;
  }

  private final SQLExpression parameter1;

  private final SQLSymbols.R_ANGLE_BRACKET greater;

  private final SQLExpression parameter2;

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
    this.parameter1.toString(stringBuilder);
    stringBuilder.append(' ');
    this.greater.toString(stringBuilder);
    stringBuilder.append(' ');
    this.parameter2.toString(stringBuilder);
  }

}
