package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLVariable extends SQLExpression.Priority0 {

  public SQLVariable(@Required final SQLSymbols.QUESTION question) {
    if (question == null) {
      throw new IllegalArgumentException("Argument [question] is null.");
    }
    this.symbol = question;
    this.index = null;
  }

  public SQLVariable(
      @Required final SQLSymbols.COLON colon,
      @Required final SQLConstant.Integer index
  ) {
    if (colon == null) {
      throw new IllegalArgumentException("Argument [colon] is null.");
    }
    this.symbol = colon;
    this.index = index;
  }

  private final Lexical.Symbol symbol;

  private final SQLConstant.Integer index;

  public SQLConstant.Integer getIndex() {
    return this.index;
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
    this.symbol.toString(stringBuilder);
    if (this.index != null) {
      this.index.toString(stringBuilder);
    }
  }

}
