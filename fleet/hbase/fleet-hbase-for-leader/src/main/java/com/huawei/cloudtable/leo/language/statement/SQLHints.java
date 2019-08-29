package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLHints extends SyntaxTree.Node {

  public SQLHints(
      @Required final SQLSymbols.SOLIDUS leftSolidus,
      @Required final SQLSymbols.STAR leftAsterisk,
      @Required final SyntaxTree.NodeList<SQLHint, SQLSymbols.COMMA> hints,
      @Required final SQLSymbols.STAR rightAsterisk,
      @Required final SQLSymbols.SOLIDUS rightSolidus
  ) {
    if (leftSolidus == null) {
      throw new IllegalArgumentException("Argument [leftSolidus] is null.");
    }
    if (leftAsterisk == null) {
      throw new IllegalArgumentException("Argument [leftAsterisk] is null.");
    }
    if (hints == null) {
      throw new IllegalArgumentException("Argument [hints] is null.");
    }
    if (rightAsterisk == null) {
      throw new IllegalArgumentException("Argument [rightAsterisk] is null.");
    }
    if (rightSolidus == null) {
      throw new IllegalArgumentException("Argument [rightSolidus] is null.");
    }
    this.leftSolidus = leftSolidus;
    this.leftAsterisk = leftAsterisk;
    this.hints = hints;
    this.rightAsterisk = rightAsterisk;
    this.rightSolidus = rightSolidus;
  }

  private final SQLSymbols.SOLIDUS leftSolidus;

  private final SQLSymbols.STAR leftAsterisk;

  private final SyntaxTree.NodeList<SQLHint, SQLSymbols.COMMA> hints;

  private final SQLSymbols.STAR rightAsterisk;

  private final SQLSymbols.SOLIDUS rightSolidus;

  public SyntaxTree.NodeList<SQLHint, SQLSymbols.COMMA> get() {
    return this.hints;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.leftSolidus.toString(stringBuilder);
    this.leftAsterisk.toString(stringBuilder);
    this.hints.toString(stringBuilder);
    this.rightAsterisk.toString(stringBuilder);
    this.rightSolidus.toString(stringBuilder);
  }

}
