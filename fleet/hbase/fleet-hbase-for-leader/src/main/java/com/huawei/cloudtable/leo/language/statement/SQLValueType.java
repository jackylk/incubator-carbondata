package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;
import com.huawei.cloudtable.leo.language.expression.SQLConstant;

public final class SQLValueType extends SyntaxTree.Node {

  public SQLValueType(@Required final SQLIdentifier name) throws SyntaxException {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    if (name.haveQuota()) {
      throw new SyntaxException(name.getPosition());
    }
    this.name = name;
    this.leftParenthesis = null;
    this.parameterList = null;
    this.rightParenthesis = null;
  }

  public SQLValueType(
      @Required final SQLIdentifier name,
      @Required final SQLSymbols.L_PARENTHESIS leftParenthesis,
      @Required final SyntaxTree.NodeList<SQLConstant, SQLSymbols.COMMA> parameterList,
      @Required final SQLSymbols.R_PARENTHESIS rightParenthesis
  ) throws SyntaxException {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    if (leftParenthesis == null) {
      throw new IllegalArgumentException("Argument [leftParenthesis] is null.");
    }
    if (parameterList == null) {
      throw new IllegalArgumentException("Argument [parameterList] is null.");
    }
    if (rightParenthesis == null) {
      throw new IllegalArgumentException("Argument [rightParenthesis] is null.");
    }
    if (name.haveQuota()) {
      throw new SyntaxException(name.getPosition());
    }
    this.name = name;
    this.leftParenthesis = leftParenthesis;
    this.parameterList = parameterList;
    this.rightParenthesis = rightParenthesis;
  }

  private final SQLIdentifier name;

  private final SQLSymbols.L_PARENTHESIS leftParenthesis;

  private final SyntaxTree.NodeList<SQLConstant, SQLSymbols.COMMA> parameterList;

  private final SQLSymbols.R_PARENTHESIS rightParenthesis;

  public SQLIdentifier getName() {
    return this.name;
  }

  public SyntaxTree.NodeList<SQLConstant, SQLSymbols.COMMA> getParameterList() {
    return this.parameterList;
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    this.toString(stringBuilder);
    return stringBuilder.toString();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.name.toString(stringBuilder);
    if (this.leftParenthesis != null) {
      this.leftParenthesis.toString(stringBuilder);
    }
    if (this.parameterList != null) {
      this.parameterList.toString(stringBuilder);
    }
    if (this.rightParenthesis != null) {
      this.rightParenthesis.toString(stringBuilder);
    }
  }

}
