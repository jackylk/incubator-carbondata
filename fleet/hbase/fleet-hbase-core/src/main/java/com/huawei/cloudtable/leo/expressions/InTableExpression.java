package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;

import javax.annotation.Nonnull;

@Function.Name(InArrayExpression.NAME)
public abstract class InTableExpression extends Evaluation<Boolean> implements Function<Boolean> {

  public static final String NAME = "IN";

  public InTableExpression(final Evaluation<Object> valueParameter, final Evaluation<Table> tableParameter) {
    super(Boolean.class, false, valueParameter, tableParameter);
    if (valueParameter == null || tableParameter == null) {
      // TODO throw parameter illegal exception.
      throw new UnsupportedOperationException();
    }
    this.name = Function.getName(this.getClass());
  }

  private final Identifier name;

  @Nonnull
  @Override
  public final Identifier getName() {
    return this.name;
  }

  private Evaluation<Object> getValueParameter() {
    return super.getParameter(0);
  }

  private Evaluation<Table> getTableParameter() {
    return super.getParameter(1);
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.getValueParameter().toString(stringBuilder);
    stringBuilder.append(" ").append(NAME).append(" ");
    stringBuilder.append("(");
    this.getTableParameter().toString(stringBuilder);
    stringBuilder.append(")");
  }

}
