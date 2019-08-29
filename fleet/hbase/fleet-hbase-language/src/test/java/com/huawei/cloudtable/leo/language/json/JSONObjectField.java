package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class JSONObjectField extends SyntaxTree.Node {

  public JSONObjectField(
    @Required final JSONConstants.String name,
    @Required final JSONSymbols.COLON colon,
    @Required final JSONElement value
  ) {
    if (name == null) {
      throw new IllegalArgumentException("Argument [name] is null.");
    }
    if (colon == null) {
      throw new IllegalArgumentException("Argument [colon] is null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Argument [value] is null.");
    }
    this.name = name;
    this.colon = colon;
    this.value = value;
  }

  private final JSONConstants.String name;

  private final JSONSymbols.COLON colon;

  private final JSONElement value;

  public JSONConstants.String getName() {
    return this.name;
  }

  public JSONSymbols.COLON getColon() {
    return this.colon;
  }

  public JSONElement getValue() {
    return this.value;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.name.toString(stringBuilder);
    this.colon.toString(stringBuilder);
    this.value.toString(stringBuilder);
  }

}
