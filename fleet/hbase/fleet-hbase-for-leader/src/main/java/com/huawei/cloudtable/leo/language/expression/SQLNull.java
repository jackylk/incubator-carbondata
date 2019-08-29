package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLNull extends SQLExpression.Priority0 {

  public SQLNull(@Required final SQLKeywords.NULL none) {
    if (none == null) {
      throw new IllegalArgumentException("Argument [none] is null.");
    }
    this.none = none;
  }

  private final SQLKeywords.NULL none;

  @Override
  public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
      final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
      final TVisitorParameter visitorParameter
  ) {
    return visitor.visit(this, visitorParameter);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.none.toString(stringBuilder);
  }

}
