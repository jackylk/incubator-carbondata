package com.huawei.cloudtable.leo.optimize;

import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.metadata.IndexDefinition;

final class IndexColumnRangeParser implements LogicalVisitor<Void, IndexColumnRangeParser.Context> {

  private static final IndexColumnRangeParser INSTANCE = new IndexColumnRangeParser();

  static void parse(final Context context, final Evaluation<Boolean> condition) {
    if (context == null) {
      throw new IllegalArgumentException("Argument [context] is null.");
    }
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    INSTANCE.visit(context, condition);
  }

  private static IndexColumnRange parse(final Context context, final RangeExpression expression) {
    for (int parameterIndex = 0; parameterIndex < expression.getParameterCount(); parameterIndex++) {
      final Evaluation<?> parameter = expression.getParameter(parameterIndex);
      if (parameter == null) {
        continue;
      }
      final IndexDefinition.Column<?> indexColumn = context.getIndexColumn(parameter);
      if(indexColumn == null) {
        continue;
      }
      final ValueRange<?> parameterRange = expression.getValueRange(parameterIndex);
      if (parameterRange == null) {
        continue;
      }
      return new IndexColumnRange(indexColumn, parameterRange);
    }
    return null;
  }

  private IndexColumnRangeParser() {
    // to do nothing.
  }

  private void visit(final Context context, final Evaluation<Boolean> condition) {
    if (condition instanceof Logical) {
      ((Logical) condition).accept(this, context);
    } else if (condition instanceof RangeExpression) {
      final IndexColumnRange indexColumnRange = parse(context, (RangeExpression) condition);
      if (indexColumnRange != null) {
        context.putResult(condition, indexColumnRange);
      }
    }
  }

  @Override
  public Void visit(final LogicalNot expression, final Context context) {
    this.visit(context, expression.getParameter());
    return null;
  }

  @Override
  public Void visit(final LogicalAnd expression, final Context context) {
    this.visit(context, expression.getParameter1());
    this.visit(context, expression.getParameter2());
    return null;
  }

  @Override
  public Void visit(final LogicalOr expression, final Context context) {
    this.visit(context, expression.getParameter1());
    this.visit(context, expression.getParameter2());
    return null;
  }

  @Override
  public Void visit(final LogicalXor expression, final Context context) {
    this.visit(context, expression.getParameter1());
    this.visit(context, expression.getParameter2());
    return null;
  }

  static abstract class Context {

    abstract IndexDefinition.Column<?> getIndexColumn(Evaluation<?> expression);

    abstract void putResult(Evaluation<?> expression, IndexColumnRange indexColumnRange);

  }

}
