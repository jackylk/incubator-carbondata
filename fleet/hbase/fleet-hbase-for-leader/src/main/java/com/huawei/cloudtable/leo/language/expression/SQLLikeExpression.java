package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLLikeExpression extends SQLExpression.Priority3 {

  public SQLLikeExpression(
      @Required final SQLExpression.Priority3 valueParameter,
      @Required final SQLKeywords.LIKE like,
      @Required final SQLExpression.Priority3 patternParameter
  ) {
    if (valueParameter == null) {
      throw new IllegalArgumentException("Argument [valueParameter] is null.");
    }
    if (like == null) {
      throw new IllegalArgumentException("Argument [like] is null.");
    }
    if (patternParameter == null) {
      throw new IllegalArgumentException("Argument [patternParameter] is null.");
    }
    this.valueParameter = valueParameter;
    this.like = like;
    this.patternParameter = patternParameter;
  }

  private final SQLExpression valueParameter;

  private final SQLKeywords.LIKE like;

  private final SQLExpression patternParameter;

  public SQLExpression getValueParameter() {
    return this.valueParameter;
  }

  public SQLExpression getPatternParameter() {
    return this.patternParameter;
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
    this.like.toString(stringBuilder);
    stringBuilder.append(' ');
    this.patternParameter.toString(stringBuilder);
  }

}
