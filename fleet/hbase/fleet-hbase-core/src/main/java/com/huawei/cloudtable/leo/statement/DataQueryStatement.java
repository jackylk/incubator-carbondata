package com.huawei.cloudtable.leo.statement;

import com.huawei.cloudtable.leo.Statement;
import com.huawei.cloudtable.leo.StatementVisitor;
import com.huawei.cloudtable.leo.Table;

import java.util.Collections;
import java.util.List;

public abstract class DataQueryStatement extends Statement<Table> {

  public DataQueryStatement(final Table.Schema resultSchema, final List<String> resultAttributeTitles) {
    if (resultSchema == null) {
      throw new IllegalArgumentException("Argument [resultSchema] is null.");
    }
    if (resultSchema.getAttributeCount() != resultAttributeTitles.size()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    this.resultSchema = resultSchema;
    this.resultAttributeTitles = Collections.unmodifiableList(resultAttributeTitles);
  }

  private final Table.Schema resultSchema;

  private final List<String> resultAttributeTitles;

  public Table.Schema getResultSchema() {
    return this.resultSchema;
  }

  public List<String> getResultAttributeTitles() {
    return this.resultAttributeTitles;
  }

  @Override
  public <TResult, TParameter> TResult accept(final StatementVisitor<TResult, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

}
