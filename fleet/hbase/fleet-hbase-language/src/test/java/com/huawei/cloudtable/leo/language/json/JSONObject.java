package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class JSONObject extends JSONElement {

  public JSONObject(
      @Required final JSONSymbols.L_BRACE leftBrace,
      @Optional final SyntaxTree.NodeList<JSONObjectField, JSONSymbols.COMMA> fields,
      @Required final JSONSymbols.R_BRACE rightBrace
      ) {
    if (leftBrace == null) {
      throw new IllegalArgumentException("Argument [leftBrace] is null.");
    }
    if (rightBrace == null) {
      throw new IllegalArgumentException("Argument [rightBrace] is null.");
    }
    this.leftBrace = leftBrace;
    this.fields = fields;
    this.rightBrace = rightBrace;
  }

  private final JSONSymbols.L_BRACE leftBrace;

  private final SyntaxTree.NodeList<JSONObjectField, JSONSymbols.COMMA> fields;

  private final JSONSymbols.R_BRACE rightBrace;

  public JSONSymbols.L_BRACE getLeftBrace() {
    return this.leftBrace;
  }

  public SyntaxTree.NodeList<JSONObjectField, JSONSymbols.COMMA> getFields() {
    return this.fields;
  }

  public JSONSymbols.R_BRACE getRightBrace() {
    return this.rightBrace;
  }

  @Override
  public void accept(final JSONElementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.leftBrace.toString(stringBuilder);
    if (this.fields != null) {
      this.fields.toString(stringBuilder);
    }
    this.leftBrace.toString(stringBuilder);
  }

}
