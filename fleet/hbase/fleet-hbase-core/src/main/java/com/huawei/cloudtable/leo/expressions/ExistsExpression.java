package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.expression.EvaluateException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;

import javax.annotation.Nonnull;

@Function.Name(ExistsExpression.NAME)
public final class ExistsExpression extends Evaluation<Boolean> implements Function<Boolean> {

  public static final String NAME = "EXISTS";

  public ExistsExpression(@ValueParameter(clazz = Table.class) final Evaluation<Table> parameter) {
    super(Boolean.class, false, parameter);
    this.name = Function.getName(this.getClass());
  }

  private final Identifier name;

  @Nonnull
  @Override
  public Identifier getName() {
    return this.name;
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
    Function.toString(this, stringBuilder);
  }

}
