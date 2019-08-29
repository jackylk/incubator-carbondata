package com.huawei.cloudtable.leo;

public abstract class ExecuteMutations {

  public abstract int getRowCount();

  public abstract void commit() throws ExecuteException;

  public abstract void rollback() throws ExecuteException;

  public abstract void join(ExecuteMutations executeMutations);

}
