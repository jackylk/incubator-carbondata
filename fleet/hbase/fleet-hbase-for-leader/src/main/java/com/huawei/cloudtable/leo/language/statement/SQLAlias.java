package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Optional;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLAlias extends SyntaxTree.Node {

  public SQLAlias(
      @Optional final SQLKeywords.AS as,
      @Required final SQLIdentifier alias
  ) {
    if (alias == null){
      throw new IllegalArgumentException("Argument [alias] is null.");
    }
    this.as = as;
    this.alias = alias;
  }

  private final SQLKeywords.AS as;

  private final SQLIdentifier alias;

  public Identifier getValue() {
    return this.alias.getValue();
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    if (this.as != null) {
      this.as.toString(stringBuilder);
      stringBuilder.append(' ');
    }
    this.alias.toString(stringBuilder);
  }

}
