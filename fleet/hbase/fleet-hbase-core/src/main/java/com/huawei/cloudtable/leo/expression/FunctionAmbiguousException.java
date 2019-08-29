package com.huawei.cloudtable.leo.expression;

import javax.annotation.Nonnull;
import java.util.List;

public class FunctionAmbiguousException extends Exception {

  private static final long serialVersionUID = 7817705568119555826L;

  FunctionAmbiguousException(@Nonnull final List<Function.Declare<?, ?>> functionDeclareList) {
    this.functionDeclareList = functionDeclareList;
  }

  private final List<Function.Declare<?, ?>> functionDeclareList;

  @Nonnull
  public List<Function.Declare<?, ?>> getFunctionDeclareList() {
    return this.functionDeclareList;
  }

}
