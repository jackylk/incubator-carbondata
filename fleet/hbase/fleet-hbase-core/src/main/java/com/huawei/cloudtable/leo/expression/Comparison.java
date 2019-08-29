package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ValueCodec;

import javax.annotation.Nonnull;

public interface Comparison<TParameter extends Comparable<TParameter>> extends RangeExpression<TParameter> {

  @Nonnull
  Class<TParameter> getParameterClass();

  @Nonnull
  Evaluation<TParameter> getParameter1();

  @Nonnull
  Evaluation<TParameter> getParameter2();

  static <TParameter extends Comparable<TParameter>> Comparator newComparator(
      final Comparison<TParameter> comparison,
      final Evaluation.CompileContext context
  ) {
    if (!context.hasHint(Evaluation.CompileHints.DISABLE_BYTE_COMPARE)
        && comparison.getParameter1() instanceof DirectGet
        && comparison.getParameter2() instanceof DirectGet) {
      final ValueCodec<TParameter> parameterCodec = context.getValueCodec(comparison.getParameterClass());
      if (parameterCodec != null && parameterCodec.isOrderPreserving()) {
        final DirectGet<TParameter> parameter1 = (DirectGet<TParameter>)comparison.getParameter1();
        final DirectGet<TParameter> parameter2 = (DirectGet<TParameter>)comparison.getParameter2();
        return new Comparator() {
          @Override
          public int compare(final Evaluation.RuntimeContext context) throws EvaluateException {
            return parameter1.evaluate(context, parameterCodec).compareTo(parameter2.evaluate(context, parameterCodec));
          }
        };
      }
    }
    return new Comparator() {
      @Override
      public int compare(final Evaluation.RuntimeContext context) throws EvaluateException {
        return comparison.getParameter1().evaluate(context).compareTo(comparison.getParameter2().evaluate(context));
      }
    };
  }

  abstract class Comparator {

    public abstract int compare(Evaluation.RuntimeContext context) throws EvaluateException;

  }

}
