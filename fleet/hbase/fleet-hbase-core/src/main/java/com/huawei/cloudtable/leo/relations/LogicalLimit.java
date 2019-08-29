package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;

import javax.annotation.Nonnull;

public final class LogicalLimit extends Relation implements Relation.Logical {

  public LogicalLimit(final Relation input, final long limit) {
    super(input.getResultSchema());
    if (limit < 0) {
      throw new IllegalArgumentException("Argument [limit] is less than 0.");
    }
    this.input = input;
    this.limit = limit;
    this.hashCode = 0;
  }

  private final Relation input;

  private final long limit;

  private volatile int hashCode;

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

  public long getLimit() {
    return this.limit;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
        this.hashCode = this.getInput().hashCode() + (int) this.limit;
    }
    return this.hashCode;
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
      final LogicalLimit that = (LogicalLimit) object;
      return this.getInput().equals(that.getInput()) && this.limit == that.limit;
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("Limit(");
    stringBuilder.append("input=").append(this.getInput().toString()).append("; ");
    stringBuilder.append("limit=").append(this.limit).append(")");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected LogicalLimit copy(final Relation[] newInputList) {
    return new LogicalLimit(newInputList[0], this.limit);
  }

}
