package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.expression.SQLConstant;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLOffset extends SyntaxTree.Node {

  public SQLOffset(
      @Required final SQLKeywords.OFFSET offset,
      @Required final SQLConstant.Integer number
  ) throws SyntaxException {
    if (offset == null) {
      throw new IllegalArgumentException("Argument [offset] is null.");
    }
    if (number == null) {
      throw new IllegalArgumentException("Argument [number] is null.");
    }
    final long value;
    try {
      value = Long.valueOf(number.getValue());
    } catch (NumberFormatException exception) {
      throw new SyntaxException(number.getPosition());
    }
    if (value < 0) {
      throw new SyntaxException(number.getPosition());
    }
    this.offset = offset;
    this.value = value;
  }

  private final SQLKeywords.OFFSET offset;

  private final long value;

  public long getValue() {
    return this.value;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.offset.toString(stringBuilder);
    stringBuilder.append(' ');
    stringBuilder.append(this.value);
  }

}
