package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLNotExpression extends SQLExpression.Priority5 {

  public SQLNotExpression(
      @Required final SQLKeywords.NOT not,
      @Required final SQLExpression.Priority5 parameter
      ) {
    if (not == null) {
      throw new IllegalArgumentException("Argument [not] is null.");
    }
    if (parameter == null) {
      throw new IllegalArgumentException("Argument [parameter] is null.");
    }
    this.not = not;
    this.parameter = parameter;
  }

  private final SQLKeywords.NOT not;

  private final SQLExpression parameter;

  public SQLExpression getParameter() {
    return this.parameter;
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
    this.not.toString(stringBuilder);
    stringBuilder.append(' ');
    this.parameter.toString(stringBuilder);
  }

}
