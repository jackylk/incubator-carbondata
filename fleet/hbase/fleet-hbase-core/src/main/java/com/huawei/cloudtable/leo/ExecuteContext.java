package com.huawei.cloudtable.leo;

import javax.annotation.Nonnull;

public interface ExecuteContext {

  @Nonnull
  String getTenantIdentifier();

  @Nonnull
  Identifier getSchemaName();

}
