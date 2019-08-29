package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.Table;

import javax.annotation.Nonnull;

public abstract class Relation {

  public Relation(final com.huawei.cloudtable.leo.Table.Schema resultSchema) {
    if (resultSchema == null) {
      throw new IllegalArgumentException("Argument [resultSchema] is null.");
    }
    this.resultSchema = resultSchema;
  }

  private final com.huawei.cloudtable.leo.Table.Schema resultSchema;

  @Nonnull
  public final Table.Schema getResultSchema() {
    return this.resultSchema;
  }

  public abstract int getInputCount();

  /**
   * @throws IndexOutOfBoundsException If input index out build bounds.
   */
  @Nonnull
  public abstract <TRelation extends com.huawei.cloudtable.leo.Relation> TRelation getInput(int inputIndex);

  public abstract <TReturn, TParameter> TReturn accept(RelationVisitor<TReturn, TParameter> visitor, TParameter parameter);

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object object);

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    this.toString(stringBuilder);
    return stringBuilder.toString();
  }

  public abstract void toString(StringBuilder stringBuilder);

  public final com.huawei.cloudtable.leo.Relation copyWithNewInputs(final com.huawei.cloudtable.leo.Relation... newInputList) {
    if (newInputList.length != this.getInputCount()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    final int inputCount = newInputList.length;
    for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
      final com.huawei.cloudtable.leo.Relation oldInput = this.getInput(inputIndex);
      final com.huawei.cloudtable.leo.Relation newInput = newInputList[inputIndex];
      if (!oldInput.resultSchema.equals(newInput.resultSchema)) {
        // TODO
        throw new UnsupportedOperationException();
      }
    }
    final com.huawei.cloudtable.leo.Relation copied = this.copy(newInputList);
    if (copied.getClass() != this.getClass()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    return copied;
  }

  protected abstract com.huawei.cloudtable.leo.Relation copy(com.huawei.cloudtable.leo.Relation[] newInputList);

  public interface Logical {
    // nothing.
  }

}
