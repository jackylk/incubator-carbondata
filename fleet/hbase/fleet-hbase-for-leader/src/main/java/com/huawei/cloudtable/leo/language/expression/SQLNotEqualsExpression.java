package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLNotEqualsExpression extends SQLExpression.Priority3 {

  public SQLNotEqualsExpression(
      @Required final Priority3 parameter1,
      @Required final SQLSymbols.EXCLAMATION not,
      @Required final SQLSymbols.EQUAL equal,
      @Required final Priority3 parameter2
      ) {
    if (parameter1 == null) {
      throw new IllegalArgumentException("Argument [parameter1] is null.");
    }
    if (not == null) {
      throw new IllegalArgumentException("Argument [not] is null.");
    }
    if (equal == null) {
      throw new IllegalArgumentException("Argument [equal] is null.");
    }
    if (parameter2 == null) {
      throw new IllegalArgumentException("Argument [parameter2] is null.");
    }
    this.parameter1 = parameter1;
    this.not = not;
    this.equal = equal;
    this.leftAngleBracket = null;
    this.rightAngleBracket = null;
    this.parameter2 = parameter2;
  }

  public SQLNotEqualsExpression(
      @Required final Priority2 parameter1,
      @Required final SQLSymbols.L_ANGLE_BRACKET leftAngleBracket,
      @Required final SQLSymbols.R_ANGLE_BRACKET rightAngleBracket,
      @Required final Priority2 parameter2
  ) {
    if (parameter1 == null) {
      throw new IllegalArgumentException("Argument [parameter1] is null.");
    }
    if (leftAngleBracket == null) {
      throw new IllegalArgumentException("Argument [leftAngleBracket] is null.");
    }
    if (rightAngleBracket == null) {
      throw new IllegalArgumentException("Argument [rightAngleBracket] is null.");
    }
    if (parameter2 == null) {
      throw new IllegalArgumentException("Argument [parameter2] is null.");
    }
    this.parameter1 = parameter1;
    this.not = null;
    this.equal = null;
    this.leftAngleBracket = leftAngleBracket;
    this.rightAngleBracket = rightAngleBracket;
    this.parameter2 = parameter2;
  }

  private final SQLExpression parameter1;

  private final SQLSymbols.EXCLAMATION not;

  private final SQLSymbols.EQUAL equal;

  private final SQLSymbols.L_ANGLE_BRACKET leftAngleBracket;

  private final SQLSymbols.R_ANGLE_BRACKET rightAngleBracket;

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
    if (this.not != null) {
      this.not.toString(stringBuilder);
    }
    if (this.equal != null) {
      this.equal.toString(stringBuilder);
    }
    if (this.leftAngleBracket != null) {
      this.leftAngleBracket.toString(stringBuilder);
    }
    if (this.rightAngleBracket != null) {
      this.rightAngleBracket.toString(stringBuilder);
    }
    stringBuilder.append(' ');
    this.parameter2.toString(stringBuilder);
  }

}
