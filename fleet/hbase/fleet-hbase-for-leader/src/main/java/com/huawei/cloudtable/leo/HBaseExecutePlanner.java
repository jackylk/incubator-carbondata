package com.huawei.cloudtable.leo;

public abstract class HBaseExecutePlanner<TExecuteResult, TStatement extends Statement<TExecuteResult>> {

  public HBaseExecutePlanner(final Class<TStatement> statementClass) {
    if (statementClass == null) {
      throw new IllegalArgumentException("Argument [statementClass] is null.");
    }
    this.statementClass = statementClass;
  }

  private final Class<TStatement> statementClass;

  public Class<TStatement> getStatementClass() {
    return this.statementClass;
  }

  public abstract ExecutePlan<? extends TExecuteResult> plan(
      HBaseExecuteEngine executeEngine,
      ExecuteContext executeContext,
      TStatement statement
  );

}
