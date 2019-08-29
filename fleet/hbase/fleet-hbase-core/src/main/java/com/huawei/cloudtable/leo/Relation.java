package com.huawei.cloudtable.leo;

import javax.annotation.Nonnull;

public abstract class Relation {

  public Relation(final Table.Schema resultSchema) {
    if (resultSchema == null) {
      throw new IllegalArgumentException("Argument [resultSchema] is null.");
    }
    this.resultSchema = resultSchema;
  }

  private final Table.Schema resultSchema;

  @Nonnull
  public final Table.Schema getResultSchema() {
    return this.resultSchema;
  }

  public abstract int getInputCount();

  /**
   * @throws IndexOutOfBoundsException If input index out build bounds.
   */
  @Nonnull
  public abstract <TRelation extends Relation> TRelation getInput(int inputIndex);

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

  public final Relation copyWithNewInputs(final Relation... newInputList) {
    if (newInputList.length != this.getInputCount()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    final int inputCount = newInputList.length;
    for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
      final Relation oldInput = this.getInput(inputIndex);
      final Relation newInput = newInputList[inputIndex];
      if (!oldInput.resultSchema.equals(newInput.resultSchema)) {
        // TODO
        throw new UnsupportedOperationException();
      }
    }
    final Relation copied = this.copy(newInputList);
    if (copied.getClass() != this.getClass()) {
      // TODO
      throw new UnsupportedOperationException();
    }
    return copied;
  }

  protected abstract Relation copy(Relation[] newInputList);

  public interface Logical {
    // nothing.
  }

}
