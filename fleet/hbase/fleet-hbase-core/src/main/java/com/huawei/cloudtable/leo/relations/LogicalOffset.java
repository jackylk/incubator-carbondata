package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;

import javax.annotation.Nonnull;

public final class LogicalOffset extends Relation implements Relation.Logical {

  public LogicalOffset(final Relation input, final long offset) {
    super(input.getResultSchema());
    if (offset < 0) {
      throw new IllegalArgumentException("Argument [offset] is less than 0.");
    }
    this.input = input;
    this.offset = offset;
    this.hashCode = 0;
  }

  private final Relation input;

  private final long offset;

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

  public long getOffset() {
    return this.offset;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
        this.hashCode = this.getInput().hashCode() + (int) this.offset;
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
      final LogicalOffset that = (LogicalOffset) object;
      return this.getInput().equals(that.getInput()) && this.offset == that.offset;
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("Offset(");
    stringBuilder.append("input=").append(this.getInput().toString()).append("; ");
    stringBuilder.append("offset=").append(this.offset).append(")");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected LogicalOffset copy(final Relation[] newInputList) {
    return new LogicalOffset(newInputList[0], this.offset);
  }

}
