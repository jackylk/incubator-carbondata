package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.FunctionManager;

public final class HBaseExpressionCodecContext implements ExpressionCodec.EncodeContext, ExpressionCodec.DecodeContext {

  HBaseExpressionCodecContext(final HBaseExecuteEngine executeEngine, final ExecuteContext executeContext) {
    this.executeEngine = executeEngine;
    this.executeContext = executeContext;
  }

  private final HBaseExecuteEngine executeEngine;

  private final ExecuteContext executeContext;

  @Override
  public HBaseValueCodecManager getValueCodecManager() {
    // TODO UDT.
    return HBaseValueCodecManager.BUILD_IN;
  }

  @Override
  public FunctionManager getFunctionManager() {
    try {
      return this.executeEngine.getFunctionManager(this.executeContext);
    } catch (ExecuteException exception) {
      // TODO
      throw new UnsupportedOperationException(exception);
    }
  }

}
