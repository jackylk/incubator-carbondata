package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.statement.SQLIdentifier;
import com.huawei.cloudtable.leo.language.annotation.Required;

public abstract class SQLReference extends SQLExpression.Priority0 {

  private SQLReference(final SQLIdentifier tableName) {
    this.tableName = tableName;
  }

  protected final SQLIdentifier tableName;

  public SQLIdentifier getTableName() {
    return this.tableName;
  }

  public static final class Column extends SQLReference {

    public Column(@Required final SQLIdentifier columnName) {
      super(null);
      if (columnName == null) {
        throw new IllegalArgumentException("Argument [columnName] is null.");
      }
      this.point = null;
      this.columnName = columnName;
    }

    public Column(
        @Required final SQLIdentifier tableName,
        @Required final SQLSymbols.POINT point,
        @Required final SQLIdentifier columnName
    ) {
      super(tableName);
      if (tableName == null) {
        throw new IllegalArgumentException("Argument [tableName] is null.");
      }
      if (point == null) {
        throw new IllegalArgumentException("Argument [point] is null.");
      }
      if (columnName == null) {
        throw new IllegalArgumentException("Argument [columnName] is null.");
      }
      this.point = point;
      this.columnName = columnName;
    }

    private final SQLSymbols.POINT point;

    private final SQLIdentifier columnName;

    public SQLIdentifier getColumnName() {
      return this.columnName;
    }

    @Override
    public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
        final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
        final TVisitorParameter visitorParameter
    ) {
      return visitor.visit(this, visitorParameter);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      if (this.tableName != null) {
        if (this.point == null) {
          throw new RuntimeException();
        }
        this.tableName.toString(stringBuilder);
        this.point.toString(stringBuilder);
      }
      this.columnName.toString(stringBuilder);
    }

  }

  public static final class ColumnAll extends SQLReference {

    public ColumnAll(@Required final SQLSymbols.STAR star) {
      super(null);
      if (star == null) {
        throw new IllegalArgumentException("Argument [star] is null.");
      }
      this.point = null;
      this.star = star;
    }

    public ColumnAll(
        @Required final SQLIdentifier tableName,
        @Required final SQLSymbols.POINT point,
        @Required final SQLSymbols.STAR star
    ) {
      super(tableName);
      if (tableName == null) {
        throw new IllegalArgumentException("Argument [tableName] is null.");
      }
      if (point == null) {
        throw new IllegalArgumentException("Argument [point] is null.");
      }
      if (star == null) {
        throw new IllegalArgumentException("Argument [star] is null.");
      }
      this.point = point;
      this.star = star;
    }

    private final SQLSymbols.POINT point;

    private final SQLSymbols.STAR star;

    @Override
    public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
        final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
        final TVisitorParameter visitorParameter
    ) {
      return visitor.visit(this, visitorParameter);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      if (this.tableName != null) {
        if (this.point == null) {
          throw new RuntimeException();
        }
        this.tableName.toString(stringBuilder);
        this.point.toString(stringBuilder);
      }
      this.star.toString(stringBuilder);
    }

  }

}
