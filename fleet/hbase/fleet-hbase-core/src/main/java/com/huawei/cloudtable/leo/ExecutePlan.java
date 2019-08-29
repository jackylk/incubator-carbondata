package com.huawei.cloudtable.leo;

public abstract class ExecutePlan<TExecuteResult> {

  public abstract TExecuteResult execute() throws ExecuteException;

  public abstract TExecuteResult execute(ExecuteParameters parameters) throws ExecuteException;

}
