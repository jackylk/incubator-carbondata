package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.expression.SQLConstant;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLLimit extends SyntaxTree.Node {

  public SQLLimit(
      @Required final SQLKeywords.LIMIT limit,
      @Required final SQLConstant.Integer number
  ) throws SyntaxException {
    if (limit == null) {
      throw new IllegalArgumentException("Argument [limit] is null.");
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
    this.limit = limit;
    this.value = value;
  }

  private final SQLKeywords.LIMIT limit;

  private final long value;

  public long getValue() {
    return this.value;
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.limit.toString(stringBuilder);
    stringBuilder.append(' ');
    stringBuilder.append(this.value);
  }

}
