package com.huawei.cloudtable.leo;

public abstract class Statement<TExecuteResult> {

  public abstract <TResult, TParameter> TResult accept(StatementVisitor<TResult, TParameter> visitor, TParameter parameter);

}
