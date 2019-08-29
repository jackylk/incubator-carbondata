package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.expression.SQLReference;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public abstract class SQLQuery extends SyntaxTree.Node {

  private SQLQuery() {
    // to do nothing.
  }

  public static final class Select extends SQLQuery {

    public Select(
        @Required final SQLKeywords.SELECT select,
        @Optional final SQLHints hints,
        @Required final SyntaxTree.NodeList<SQLProject, SQLSymbols.COMMA> projects,
        @Optional final SQLFrom from,
        @Optional final SQLWhere where,
        @Optional final SQLGroupBy groupBy,
        @Optional final SQLHaving having
    ) throws SyntaxException {
      if (select == null) {
        throw new IllegalArgumentException("Argument [select] is null.");
      }
      if (projects == null) {
        throw new IllegalArgumentException("Argument [projects] is null.");
      }
      for (int index = 0; index < projects.size(); index++) {
        final SQLProject project = projects.get(index);
        if (project.getExpression() instanceof SQLReference.ColumnAll && project.getAlias() != null) {
          throw new SyntaxException(project.getPosition());
        }
      }
      this.select = select;
      this.hints = hints;
      this.projects = projects;
      this.from = from;
      this.where = where;
      this.groupBy = groupBy;
      this.having = having;
    }

    private final SQLKeywords.SELECT select;

    private final SQLHints hints;

    private final SyntaxTree.NodeList<SQLProject, SQLSymbols.COMMA> projects;

    private final SQLFrom from;

    private final SQLWhere where;

    private final SQLGroupBy groupBy;

    private final SQLHaving having;

    public SQLHints getHints() {
      return this.hints;
    }

    public SyntaxTree.NodeList<SQLProject, SQLSymbols.COMMA> getProjects() {
      return this.projects;
    }

    public SQLFrom getFrom() {
      return this.from;
    }

    public SQLWhere getWhere() {
      return this.where;
    }

    public SQLGroupBy getGroupBy() {
      return this.groupBy;
    }

    public SQLHaving getHaving() {
      return this.having;
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.select.toString(stringBuilder);
      if (this.hints != null) {
        stringBuilder.append(' ');
        this.hints.toString(stringBuilder);
      }
      stringBuilder.append(' ');
      this.projects.toString(stringBuilder);
      if (this.from != null) {
        stringBuilder.append(' ');
        this.from.toString(stringBuilder);
      }
      if (this.where != null) {
        stringBuilder.append(' ');
        this.where.toString(stringBuilder);
      }
      if (this.groupBy != null) {
        stringBuilder.append(' ');
        this.groupBy.toString(stringBuilder);
      }
      if (this.having != null) {
        stringBuilder.append(' ');
        this.having.toString(stringBuilder);
      }
    }

  }

}
