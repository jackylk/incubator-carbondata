package com.huawei.cloudtable.leo.statement;

import com.huawei.cloudtable.leo.ExecuteMutations;
import com.huawei.cloudtable.leo.Statement;
import com.huawei.cloudtable.leo.StatementVisitor;

public abstract class DataManipulationStatement extends Statement<ExecuteMutations> {

  @Override
  public <TResult, TParameter> TResult accept(final StatementVisitor<TResult, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
