package com.huawei.cloudtable.leo.language.json;

public final class JSONNull extends JSONElement {

  public JSONNull(final JSONKeywords.NULL none) {
    this.none = none;
  }

  private final JSONKeywords.NULL none;

  @Override
  public void accept(final JSONElementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append(this.none.toString());
  }

}
