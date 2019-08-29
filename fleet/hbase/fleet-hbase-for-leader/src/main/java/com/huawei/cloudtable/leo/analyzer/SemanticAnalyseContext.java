package com.huawei.cloudtable.leo.analyzer;

import com.huawei.cloudtable.leo.ExecuteContext;
import com.huawei.cloudtable.leo.ExecuteEngine;

import com.huawei.cloudtable.leo.ExecuteException;
import com.huawei.cloudtable.leo.expression.FunctionManager;
import javax.annotation.Nonnull;

public abstract class SemanticAnalyseContext {

  public static SemanticAnalyseContext newDefault(
      final String statementString,
      final ExecuteEngine executeEngine,
      final ExecuteContext executeContext
      ) {
    return new SemanticAnalyseContext() {
      @Nonnull
      @Override
      public String getStatementString() {
        return statementString;
      }

      @Nonnull
      @Override
      public ExecuteEngine getExecuteEngine() {
        return executeEngine;
      }

      @Nonnull
      @Override
      public ExecuteContext getExecuteContext() {
        return executeContext;
      }

      @Nonnull
      @Override
      public FunctionManager getFunctionManager()  throws ExecuteException {
        return executeEngine.getFunctionManager(executeContext);
      }
    };
  }

  @Nonnull
  public abstract String getStatementString();

  @Nonnull
  public abstract ExecuteEngine getExecuteEngine();

  @Nonnull
  public abstract ExecuteContext getExecuteContext();

  @Nonnull
  public abstract FunctionManager getFunctionManager() throws ExecuteException;

}
