package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLIdentifier extends SyntaxTree.Node {

  public SQLIdentifier(@Required final Lexical.Word value) {
    if (value == null) {
      throw new IllegalArgumentException("Argument [value] is null.");
    }
    this.haveQuota = false;
    this.valueWord = value;
    this.value = Identifier.of(value.getValue());
  }

  public SQLIdentifier(@Required final LiteralWithQuota1 value) {
    if (value == null) {
      throw new IllegalArgumentException("Argument [value] is null.");
    }
    this.haveQuota = true;
    this.valueWord = value;
    this.value = Identifier.of(value.getValue());
  }

  public SQLIdentifier(final LiteralWithQuota2 value) {
    if (value == null) {
      throw new IllegalArgumentException("Argument [value] is null.");
    }
    this.haveQuota = true;
    this.valueWord = value;
    this.value = Identifier.of(value.getValue());
  }

  private final boolean haveQuota;

  private final Lexical.Word valueWord;

  private final Identifier value;

  public Identifier getValue() {
    return this.value;
  }

  public boolean haveQuota() {
    return this.haveQuota;
  }

  @Override
  public String toString() {
    return this.valueWord.toString();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.valueWord.toString(stringBuilder);
  }

  @Lexical.Word.WithQuota.Declare(quota = SQLSymbols.DOUBLE_QUOTA.class, escape = SQLSymbols.COMMA.class)
  public static final class LiteralWithQuota1 extends Lexical.Word.WithQuota {

    public LiteralWithQuota1(final String value) {
      super(value);
    }

  }

  @Lexical.Word.WithQuota.Declare(quota = SQLSymbols.CEDILLA.class, escape = SQLSymbols.COMMA.class)
  public static final class LiteralWithQuota2 extends Lexical.Word.WithQuota {

    public LiteralWithQuota2(final String value) {
      super(value);
    }

  }

}
