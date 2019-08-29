package com.huawei.cloudtable.leo.language.statements;

import com.huawei.cloudtable.leo.language.SQLStatement;
import com.huawei.cloudtable.leo.language.statement.SQLLimit;
import com.huawei.cloudtable.leo.language.statement.SQLOffset;
import com.huawei.cloudtable.leo.language.statement.SQLQuery;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLSelectStatement extends SQLStatement {

  public SQLSelectStatement(
      @Required final SQLQuery query,
      @Optional final SQLLimit limit,
      @Optional final SQLOffset offset
  ) {
    if (query == null) {
      throw new IllegalArgumentException("Argument [query] is null.");
    }
    this.query = query;
    this.limit = limit;
    this.offset = offset;
  }

  private final SQLQuery query;

  private final SQLLimit limit;

  private final SQLOffset offset;

  public SQLQuery getQuery() {
    return this.query;
  }

  public SQLOffset getOffset() {
    return this.offset;
  }

  public SQLLimit getLimit() {
    return this.limit;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.query.toString(stringBuilder);
    if (this.limit != null) {
      stringBuilder.append(' ');
      this.limit.toString(stringBuilder);
    }
    if (this.offset != null) {
      stringBuilder.append(' ');
      this.offset.toString(stringBuilder);
    }
  }

}
