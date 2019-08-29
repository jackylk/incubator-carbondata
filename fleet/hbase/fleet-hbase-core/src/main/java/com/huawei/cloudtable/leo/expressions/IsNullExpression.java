package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.*;

import javax.annotation.Nonnull;

@Function.Name(IsNullExpression.NAME)
public final class IsNullExpression extends Evaluation<Boolean> implements Function<Boolean> {

  public static final String NAME = "IS_NULL";

  public IsNullExpression(@ValueParameter(clazz = Object.class) final Evaluation<?> parameter) {
    super(Boolean.class, false, parameter);
    this.name = Function.getName(this.getClass());
  }

  private final Identifier name;

  private Evaluator<Boolean> evaluator;

  @Nonnull
  @Override
  public Identifier getName() {
    return this.name;
  }

  private Evaluation<Object> getParameter() {
    return super.getParameter(0);
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);
    this.evaluator = this.newEvaluator(context);
  }

  @Override
  public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
    return this.evaluator.evaluate(context);
  }

  @Nonnull
  private Evaluator<Boolean> newEvaluator(final CompileContext context) {
    final Evaluation<Object> parameter = this.getParameter();
    if (parameter == null) {
      return Evaluator.ALWAYS_TRUE_EVALUATOR;
    }
    if (parameter instanceof Constant) {
      return Evaluator.ALWAYS_FALSE_EVALUATOR;
    }
    if (!context.hasHint(CompileHints.DISABLE_BYTE_COMPARE) && this.getParameter() instanceof DirectGet) {
      final ValueCodec<Object> parameterCodec = context.getValueCodec(parameter.getResultClass());
      if (parameterCodec != null) {
        return new Evaluator<Boolean>() {
          @Override
          public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
            return ((DirectGet<Object>) parameter).evaluate(context, parameterCodec) == null;
          }
        };
      }
    }
    return new Evaluator<Boolean>() {
      @Override
      public Boolean evaluate(final RuntimeContext context) throws EvaluateException {
        return parameter.evaluate(context) == null;
      }
    };
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    if (this.getParameter() == null) {
      stringBuilder.append("NULL");
    } else {
      this.getParameter().toString(stringBuilder);
    }
    stringBuilder.append(" IS NULL");
  }

}
