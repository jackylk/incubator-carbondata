package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.annotation.Required;

public abstract class SQLConstant extends SQLExpression.Priority0 {

  private SQLConstant() {
    // to do nothing.
  }

  @Override
  public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
      final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
      final TVisitorParameter visitorParameter
  ) {
    return visitor.visit(this, visitorParameter);
  }

  @Lexical.Word.WithQuota.Declare(quota = SQLSymbols.SINGLE_QUOTA.class, escape = SQLSymbols.COMMA.class)
  public static final class StringWord extends Lexical.Word.WithQuota {

    public StringWord(final java.lang.String value) {
      super(value);
    }

  }

  public static final class String extends SQLConstant {

    public String(@Required final StringWord value) {
      if (value == null) {
        throw new IllegalArgumentException("Argument [value] is null.");
      }
      this.value = value;
    }

    private final StringWord value;

    public java.lang.String getValue() {
      return this.value.getValue();
    }

    @Override
    public java.lang.String toString() {
      return this.value.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.value.toString(stringBuilder);
    }

  }

  @Lexical.Word.WithStart.Declare(startCharacters = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}, legalCharacters = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'})
  public static final class NumberWord extends Lexical.Word.WithStart {

    public NumberWord(final java.lang.String value) {
      super(value);
    }

  }

  public static final class Integer extends SQLConstant {

    public Integer(@Required final NumberWord number) {
      if (number == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      this.plus = null;
      this.subtraction = null;
      this.number = number;
    }

    public Integer(
        @Required final SQLSymbols.PLUS plus,
        @Required final NumberWord number
    ) {
      if (plus == null) {
        throw new IllegalArgumentException("Argument [plus] is null.");
      }
      if (number == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      this.plus = plus;
      this.subtraction = null;
      this.number = number;
    }

    public Integer(
        @Required final SQLSymbols.SUBTRACTION subtraction,
        @Required final NumberWord number
    ) {
      if (subtraction == null) {
        throw new IllegalArgumentException("Argument [subtraction] is null.");
      }
      if (number == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      this.plus = null;
      this.subtraction = subtraction;
      this.number = number;
    }

    private final SQLSymbols.PLUS plus;

    private final SQLSymbols.SUBTRACTION subtraction;

    private final NumberWord number;

    public java.lang.String getValue() {
      return this.subtraction == null ? this.number.getValue() : this.subtraction.toString() + this.number.getValue();
    }

    @Override
    public java.lang.String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      this.toString(stringBuilder);
      return stringBuilder.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      if (this.plus != null) {
        this.plus.toString(stringBuilder);
      }
      if (this.subtraction != null) {
        this.subtraction.toString(stringBuilder);
      }
      this.number.toString(stringBuilder);
    }

  }

  public static final class Real extends SQLConstant {

    public Real(
        @Required final NumberWord integer,
        @Required final SQLSymbols.POINT point,
        @Required final NumberWord decimal
    ) {
      if (integer == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      if (point == null) {
        throw new IllegalArgumentException("Argument [point] is null.");
      }
      if (decimal == null) {
        throw new IllegalArgumentException("Argument [decimal] is null.");
      }
      this.plus = null;
      this.subtraction = null;
      this.integer = integer;
      this.point = point;
      this.decimal = decimal;
    }

    public Real(
        @Required final SQLSymbols.PLUS plus,
        @Required final NumberWord integer,
        @Required final SQLSymbols.POINT point,
        @Required final NumberWord decimal
    ) {
      if (plus == null) {
        throw new IllegalArgumentException("Argument [plus] is null.");
      }
      if (integer == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      if (point == null) {
        throw new IllegalArgumentException("Argument [point] is null.");
      }
      if (decimal == null) {
        throw new IllegalArgumentException("Argument [decimal] is null.");
      }
      this.plus = plus;
      this.subtraction = null;
      this.integer = integer;
      this.point = point;
      this.decimal = decimal;
    }

    public Real(
        @Required final SQLSymbols.SUBTRACTION subtraction,
        @Required final NumberWord integer,
        @Required final SQLSymbols.POINT point,
        @Required final NumberWord decimal
    ) {
      if (subtraction == null) {
        throw new IllegalArgumentException("Argument [subtraction] is null.");
      }
      if (integer == null) {
        throw new IllegalArgumentException("Argument [integer] is null.");
      }
      if (point == null) {
        throw new IllegalArgumentException("Argument [point] is null.");
      }
      if (decimal == null) {
        throw new IllegalArgumentException("Argument [decimal] is null.");
      }
      this.plus = null;
      this.subtraction = subtraction;
      this.integer = integer;
      this.point = point;
      this.decimal = decimal;
    }

    private final SQLSymbols.PLUS plus;

    private final SQLSymbols.SUBTRACTION subtraction;

    private final NumberWord integer;

    private final SQLSymbols.POINT point;

    private final NumberWord decimal;

    public java.lang.String getValue() {
      if (this.subtraction == null) {
        return this.integer.getValue() + this.point.toString() + this.decimal.toString();
      } else {
        return this.subtraction.toString() + this.integer.getValue() + this.point.toString() + this.decimal.toString();
      }
    }

    @Override
    public java.lang.String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      this.toString(stringBuilder);
      return stringBuilder.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      if (this.plus != null) {
        this.plus.toString(stringBuilder);
      }
      if (this.subtraction != null) {
        this.subtraction.toString(stringBuilder);
      }
      this.integer.toString(stringBuilder);
      this.point.toString(stringBuilder);
      this.decimal.toString(stringBuilder);
    }

  }

  public static final class Boolean extends SQLConstant {

    public Boolean(@Required final SQLKeywords.TRUE value) {
      if (value == null) {
        throw new IllegalArgumentException("Argument [value] is null.");
      }
      this.value = value;
    }

    public Boolean(@Required final SQLKeywords.FALSE value) {
      if (value == null) {
        throw new IllegalArgumentException("Argument [value] is null.");
      }
      this.value = value;
    }

    private final Lexical.Keyword value;

    public java.lang.String getValue() {
      return this.value.toString();
    }

    @Override
    public java.lang.String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      this.toString(stringBuilder);
      return stringBuilder.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.value.toString(stringBuilder);
    }

  }

}
