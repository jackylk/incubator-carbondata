package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class JSONArray extends JSONElement {

  public JSONArray(
      @Required final JSONSymbols.L_SQUARE_BRACKET leftSquareBracket,
      @Optional final SyntaxTree.NodeList<JSONElement, JSONSymbols.COMMA> elements,
      @Required final JSONSymbols.R_SQUARE_BRACKET rightSquareBracket
  ) {
    if (leftSquareBracket == null) {
      throw new IllegalArgumentException("Argument [leftSquareBracket] is null.");
    }
    if (rightSquareBracket == null) {
      throw new IllegalArgumentException("Argument [rightSquareBracket] is null.");
    }
    this.leftSquareBracket = leftSquareBracket;
    this.elements = elements;
    this.rightSquareBracket = rightSquareBracket;
  }

  private final JSONSymbols.L_SQUARE_BRACKET leftSquareBracket;

  private final SyntaxTree.NodeList<JSONElement, JSONSymbols.COMMA> elements;

  private final JSONSymbols.R_SQUARE_BRACKET rightSquareBracket;

  public JSONSymbols.L_SQUARE_BRACKET getLeftSquareBracket() {
    return this.leftSquareBracket;
  }

  public SyntaxTree.NodeList<JSONElement, JSONSymbols.COMMA> getElements() {
    return this.elements;
  }

  public JSONSymbols.R_SQUARE_BRACKET getRightSquareBracket() {
    return this.rightSquareBracket;
  }

  @Override
  public void accept(final JSONElementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.leftSquareBracket.toString(stringBuilder);
    if (this.elements != null) {
      this.elements.toString(stringBuilder);
    }
    this.rightSquareBracket.toString(stringBuilder);
  }

}
