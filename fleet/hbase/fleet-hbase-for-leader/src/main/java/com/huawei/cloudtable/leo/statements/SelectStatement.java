package com.huawei.cloudtable.leo.statements;

import com.huawei.cloudtable.leo.Relation;

import com.huawei.cloudtable.leo.statement.DataQueryStatement;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class SelectStatement extends DataQueryStatement {

  public SelectStatement(final Relation relation, final List<String> relationAttributeTitles, final Set<String> hints) {
    super(relation.getResultSchema(), relationAttributeTitles);
    this.relation = relation;
    this.hints = Collections.unmodifiableSet(hints);
  }

  private final Relation relation;

  private final Set<String> hints;

  public Relation getRelation() {
    return this.relation;
  }

  public Set<String> getHints() {
    return this.hints;
  }

}
