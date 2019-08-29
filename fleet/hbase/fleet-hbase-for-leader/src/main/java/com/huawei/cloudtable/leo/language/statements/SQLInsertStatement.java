package com.huawei.cloudtable.leo.language.statements;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLStatement;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;
import com.huawei.cloudtable.leo.language.statement.SQLHints;
import com.huawei.cloudtable.leo.language.statement.SQLIdentifier;
import com.huawei.cloudtable.leo.language.statement.SQLTableIdentifier;
import com.huawei.cloudtable.leo.language.statement.SQLValues;

public abstract class SQLInsertStatement<TSource extends SyntaxTree.Node> extends SQLStatement {

  private SQLInsertStatement(
      @Required final SQLKeywords.INSERT insert,
      @Required final SQLKeywords.INTO into,
      @Optional final SQLHints hints,
      @Required final SQLTableIdentifier table,
      @Optional final SyntaxTree.NodeList<SQLIdentifier, SQLSymbols.COMMA> columns,
      @Required final TSource source
  ) {
    if (insert == null) {
      throw new IllegalArgumentException("Argument [insert] is null.");
    }
    if (into == null) {
      throw new IllegalArgumentException("Argument [into] is null.");
    }
    if (table == null) {
      throw new IllegalArgumentException("Argument [table] is null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Argument [source] is null.");
    }
    this.insert = insert;
    this.into = into;
    this.hints = hints;
    this.table = table;
    this.columns = columns;
    this.source = source;
  }

  protected final SQLKeywords.INSERT insert;

  protected final SQLKeywords.INTO into;

  protected final SQLHints hints;

  protected final SQLTableIdentifier table;

  protected final SyntaxTree.NodeList<SQLIdentifier, SQLSymbols.COMMA> columns;

  protected final TSource source;

  public SQLHints getHints() {
    return this.hints;
  }

  public SQLTableIdentifier getTable() {
    return this.table;
  }

  public SyntaxTree.NodeList<SQLIdentifier, SQLSymbols.COMMA> getColumns() {
    return this.columns;
  }

  public TSource getSource() {
    return this.source;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.insert.toString(stringBuilder);
    stringBuilder.append(' ');
    this.into.toString(stringBuilder);
    if (this.hints != null) {
      stringBuilder.append(' ');
      this.hints.toString(stringBuilder);
    }
    stringBuilder.append(' ');
    this.table.toString(stringBuilder);
    if (this.columns != null) {
      stringBuilder.append(' ');
      this.columns.toString(stringBuilder);
    }
    stringBuilder.append(' ');
    this.source.toString(stringBuilder);
  }

  public static final class FromValues extends SQLInsertStatement<SyntaxTree.NodeList<SQLValues, SQLSymbols.COMMA>> {

    public FromValues(
        @Required final SQLKeywords.INSERT insert,
        @Required final SQLKeywords.INTO into,
        @Optional final SQLHints hints,
        @Required final SQLTableIdentifier table,
        @Optional final SyntaxTree.NodeList<SQLIdentifier, SQLSymbols.COMMA> columns,
        @Optional final SQLKeywords.VALUES values,
        @Optional final SyntaxTree.NodeList<SQLValues, SQLSymbols.COMMA> valuesList
        ) {
      super(insert, into, hints, table, columns, valuesList);
      if (values == null) {
        throw new IllegalArgumentException("Argument [values] is null.");
      }
      this.values = values;
    }

    private final SQLKeywords.VALUES values;

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.insert.toString(stringBuilder);
      stringBuilder.append(' ');
      this.into.toString(stringBuilder);
      if (this.hints != null) {
        stringBuilder.append(' ');
        this.hints.toString(stringBuilder);
      }
      stringBuilder.append(' ');
      this.table.toString(stringBuilder);
      if (this.columns != null) {
        stringBuilder.append(' ');
        this.columns.toString(stringBuilder);
      }
      stringBuilder.append(' ');
      this.values.toString(stringBuilder);
      stringBuilder.append(' ');
      this.source.toString(stringBuilder);
    }

  }

}
