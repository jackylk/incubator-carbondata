package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.ExpressionVisitor;

import java.util.List;

public final class Case<TResult> extends Evaluation<TResult> {
  private List<Branch<TResult>> branches;
  private Evaluation<TResult> operateElse;

  /**
   * to get the result type/class
   *
   * @param branches the WHEN THEN expression
   * @param <TResult> the result type/class
   * @return result class
   */
  private static <TResult> Class<TResult> getResultClass(final List<Branch<TResult>> branches) {
    for (Branch<TResult> branch : branches) {
      if (branch.getOperate() != null) {
        return branch.getOperate().getResultClass();
      }
    }

    return (Class<TResult>) Object.class;
  }

  public Case(final List<Branch<TResult>> branches, final Evaluation<TResult> operateElse) {
    super(getResultClass(branches), false);
    this.branches = branches;
    this.operateElse = operateElse;

    if (branches == null || branches.isEmpty()) {
      throw new IllegalArgumentException("Argument [branches] is null.");
    }
  }

  public List<Branch<TResult>> getBranches() {
    return branches;
  }

  public Evaluation<TResult> getOperateElse() {
    return operateElse;
  }

  @Override
  public void compile(final CompileContext context) {
    super.compile(context);

    for (Branch<TResult> branch : branches) {
      branch.getCondition().compile(context);
      if (branch.getOperate() != null) {
        branch.getOperate().compile(context);
      }
    }

    if (operateElse != null) {
      operateElse.compile(context);
    }
  }

  @Override
  public TResult evaluate(final RuntimeContext context) throws EvaluateException {
    for (Branch<TResult> branch : branches) {
      if (branch.getCondition().evaluate(context)) {
        Evaluation<TResult> evaluation = branch.getOperate();
        if (evaluation != null) {
          return evaluation.evaluate(context);
        } else {
          return null;
        }
      }
    }

    if (operateElse != null) {
      return operateElse.evaluate(context);
    } else {
      return null;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("CASE");
    for (Branch<TResult> branch : branches) {
      stringBuilder.append(" WHEN ");
      stringBuilder.append(branch.getCondition());
      stringBuilder.append(" THEN ");
      stringBuilder.append(branch.getOperate());
    }

    if (operateElse != null) {
      stringBuilder.append(" ELSE ");
      stringBuilder.append(operateElse);
    }

    stringBuilder.append(" END");
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof Case) || getBranches().size() != ((Case)object).getBranches().size()) {
      return false;
    }

    Case<TResult> that = (Case)object;
    for (int i = 0; i < getBranches().size(); i++) {
      Evaluation<Boolean> thisCondition = getBranches().get(i).getCondition();
      Evaluation<Boolean> thatCondition = that.getBranches().get(i).getCondition();
      if (!thisCondition.equals(thatCondition)) {
        return false;
      }

      Evaluation<TResult> thisOperate = getBranches().get(i).getOperate();
      Evaluation<TResult> thatOperate = that.getBranches().get(i).getOperate();
      if (!compareOperate(thisOperate, thatOperate)) {
        return false;
      }
    }

    return compareOperate(getOperateElse(), that.getOperateElse());
  }

  private boolean compareOperate(Evaluation<TResult> thisOperate, Evaluation<TResult> thatOperate) {
    if (thisOperate == null) {
      return thatOperate == null;
    } else {
      return thisOperate.equals(thatOperate);
    }
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final ExpressionVisitor<TReturn, TParameter> visitor,
                                              final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  public static class Branch<TResult> {
    private Evaluation<Boolean> condition;

    private Evaluation<TResult> operate;

    public Branch(Evaluation<Boolean> condition, Evaluation<TResult> operate) {
      this.condition = condition;
      this.operate = operate;
    }

    public Evaluation<Boolean> getCondition() {
      return condition;
    }

    public Evaluation<TResult> getOperate() {
      return operate;
    }

  }
}
