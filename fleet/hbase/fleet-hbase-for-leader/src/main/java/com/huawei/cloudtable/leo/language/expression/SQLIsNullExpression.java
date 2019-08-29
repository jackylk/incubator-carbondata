package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLIsNullExpression extends SQLExpression.Priority3 {

  public SQLIsNullExpression(
      @Required final Priority3 valueParameter,
      @Required final SQLKeywords.IS is,
      @Required final SQLKeywords.NULL none
  ) {
    if (valueParameter == null) {
      throw new IllegalArgumentException("Argument [valueParameter] is null.");
    }
    if (is == null) {
      throw new IllegalArgumentException("Argument [is] is null.");
    }
    if (none == null) {
      throw new IllegalArgumentException("Argument [none] is null.");
    }
    this.valueParameter = valueParameter;
    this.is = is;
    this.none = none;
  }

  private final SQLExpression valueParameter;

  private final SQLKeywords.IS is;

  private final SQLKeywords.NULL none;

  public SQLExpression getValueParameter() {
    return this.valueParameter;
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
    this.is.toString(stringBuilder);
    stringBuilder.append(' ');
    this.none.toString(stringBuilder);
  }

}
