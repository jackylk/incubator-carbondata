package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.expression.Evaluation;

import javax.annotation.Nonnull;

public final class LogicalFilter extends Relation implements Relation.Logical {

  public LogicalFilter(final Relation input, final Evaluation<Boolean> condition) {
    super(input.getResultSchema());
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    this.input = input;
    this.condition = condition;
  }

  private final Relation input;

  private final Evaluation<Boolean> condition;

  @SuppressWarnings("unchecked")
  public <TRelation extends Relation> TRelation getInput() {
    return (TRelation) this.input;
  }

  @Override
  public int getInputCount() {
    return 1;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @Override
  public <TRelation extends Relation> TRelation getInput(final int inputIndex) {
    if (inputIndex == 0) {
      return (TRelation) this.input;
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public Evaluation<Boolean> getCondition() {
    return this.condition;
  }

  @Override
  public int hashCode() {
    return 31 * this.getInput().hashCode() + this.condition.hashCode();
  }

  @Override
  public boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (object.getClass() == this.getClass()) {
      final LogicalFilter that = (LogicalFilter) object;
      return this.getInput().equals(that.getInput()) && this.condition.equals(that.condition);
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("Filter(");
    stringBuilder.append("input=").append(this.getInput().toString()).append("; ");
    stringBuilder.append("condition=\"");
    this.condition.toString(stringBuilder);
    stringBuilder.append("\")");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected LogicalFilter copy(final Relation[] newInputList) {
    return new LogicalFilter(newInputList[0], this.condition);
  }

}
