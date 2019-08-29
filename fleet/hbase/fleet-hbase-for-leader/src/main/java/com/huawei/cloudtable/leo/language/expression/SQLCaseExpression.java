package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;

public final class SQLCaseExpression extends SQLExpression.Priority4 {
  public SQLCaseExpression(
    @Required final SQLKeywords.CASE caze,
    @Required final SyntaxTree.NodeList<Branch, Lexical.Whitespace> branches,
    @Required final SQLKeywords.END end
  ) {
    if (caze == null) {
      throw new IllegalArgumentException("Argument [case] is null.");
    }
    if (branches == null || branches.isEmpty()) {
      throw new IllegalArgumentException("Argument [branches] is null or empty.");
    }
    if (end == null) {
      throw new IllegalArgumentException("Argument [end] is null.");
    }
    this.caze = caze;
    this.branches = branches;
    this.elze = null;
    this.operateElse = null;
    this.end = end;
  }


  public SQLCaseExpression(
    @Required final SQLKeywords.CASE caze,
    @Required final SyntaxTree.NodeList<Branch, Lexical.Whitespace> branches,
    @Required final SQLKeywords.ELSE elze,
    @Required final SQLExpression.Priority4 operateElse,
    @Required final SQLKeywords.END end
  ) {
    if (caze == null) {
      throw new IllegalArgumentException("Argument [case] is null.");
    }
    if (branches == null || branches.isEmpty()) {
      throw new IllegalArgumentException("Argument [branches] is null or empty.");
    }
    if (elze == null) {
      throw new IllegalArgumentException("Argument [else] is null.");
    }
    if (operateElse == null) {
      throw new IllegalArgumentException("Argument [else operation] is null.");
    }
    if (end == null) {
      throw new IllegalArgumentException("Argument [end] is null.");
    }
    this.caze = caze;
    this.branches = branches;
    this.elze = elze;
    this.operateElse = operateElse;
    this.end = end;
  }

  private final SQLKeywords.CASE caze;

  private final SyntaxTree.NodeList<Branch, Lexical.Whitespace> branches;

  private final SQLKeywords.ELSE elze;

  private final SQLExpression operateElse;

  private final SQLKeywords.END end;

  public SyntaxTree.NodeList<Branch, Lexical.Whitespace> getBranches() {
    return this.branches;
  }

  public SQLExpression getOperateElse() {
    return this.operateElse;
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
    this.caze.toString(stringBuilder);
    stringBuilder.append(' ');
    this.branches.toString(stringBuilder);
    if (this.elze != null && this.operateElse != null) {
      stringBuilder.append(' ');
      this.elze.toString(stringBuilder);
      stringBuilder.append(' ');
      this.operateElse.toString(stringBuilder);
    }
    stringBuilder.append(' ');
    this.end.toString(stringBuilder);
  }

  public static final class Branch extends SyntaxTree.Node {
    public Branch(
      @Required final SQLKeywords.WHEN when,
      @Required final SQLExpression.Priority4 condition,
      @Required final SQLKeywords.THEN then,
      @Required final SQLExpression.Priority4 operate
    ) {
      if (when == null) {
        throw new IllegalArgumentException("Argument [when] is null.");
      }
      if (condition == null) {
        throw new IllegalArgumentException("Argument [when condition] is null.");
      }
      if (then == null) {
        throw new IllegalArgumentException("Argument [then] is null.");
      }
      if (operate == null) {
        throw new IllegalArgumentException("Argument [then operation] is null.");
      }
      this.when = when;
      this.condition = condition;
      this.then = then;
      this.operate = operate;
    }

    private final SQLKeywords.WHEN when;

    private final SQLExpression condition;

    private final SQLKeywords.THEN then;

    private final SQLExpression operate;

    public SQLExpression getCondition() {
      return this.condition;
    }

    public SQLExpression getOperate() {
      return this.operate;
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.when.toString(stringBuilder);
      stringBuilder.append(' ');
      this.condition.toString(stringBuilder);
      stringBuilder.append(' ');
      this.then.toString(stringBuilder);
      stringBuilder.append(' ');
      this.operate.toString(stringBuilder);
    }

  }
}
