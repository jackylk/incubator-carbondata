package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;

public final class JSONConstants {

  @Lexical.Word.WithQuota.Declare(quota = JSONSymbols.DOUBLE_QUOTA.class, escape = JSONSymbols.COMMA.class)
  public static final class StringWord extends Lexical.Word.WithQuota {

    public StringWord(final java.lang.String value) {
      super(value);
    }

  }

  @Lexical.Word.WithStart.Declare(startCharacters = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}, legalCharacters = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'})
  public static final class NumberWord extends Lexical.Word.WithStart {

    public NumberWord(final java.lang.String value) {
      super(value);
    }

  }

  public static final class String extends JSONElement {

    public String(final StringWord value) {
      this.value = value;
    }

    private final StringWord value;

    @Override
    public void accept(final JSONElementVisitor visitor) {
      visitor.visit(this);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.value.toString(stringBuilder);
    }

  }

  public static final class Integer extends JSONElement {

    public Integer(final NumberWord value) {
      this.value = value;
    }

    private final NumberWord value;

    @Override
    public void accept(final JSONElementVisitor visitor) {
      visitor.visit(this);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.value.toString(stringBuilder);
    }

  }

  public static final class Real extends JSONElement {

    public Real(
        @Optional final NumberWord integer,
        final JSONSymbols.POINT point,
        final NumberWord decimal
    ) {
      this.integer = integer;
      this.point = point;
      this.decimal = decimal;
    }

    private final NumberWord integer;

    private final JSONSymbols.POINT point;

    private final NumberWord decimal;

    @Override
    public void accept(final JSONElementVisitor visitor) {
      visitor.visit(this);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      if (this.integer != null) {
        this.integer.toString(stringBuilder);
      }
      this.point.toString(stringBuilder);
      this.decimal.toString(stringBuilder);
    }

  }

  public static final class Boolean extends JSONElement {

    public Boolean(final JSONKeywords.TRUE value) {
      this.value = value;
    }

    public Boolean(final JSONKeywords.FALSE value) {
      this.value = value;
    }

    private final SyntaxTree.Node value;

    @Override
    public void accept(final JSONElementVisitor visitor) {
      visitor.visit(this);
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.value.toString(stringBuilder);
    }

  }

}
