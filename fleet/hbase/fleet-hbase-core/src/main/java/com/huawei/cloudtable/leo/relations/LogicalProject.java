package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.expression.Evaluation;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public final class LogicalProject extends Relation implements Relation.Logical {

  private static final ThreadLocal<Table.SchemaBuilder> SCHEMA_BUILDER_CACHE = new ThreadLocal<Table.SchemaBuilder>(){
    @Override
    protected Table.SchemaBuilder initialValue() {
      return new Table.SchemaBuilder();
    }
  };

  public LogicalProject(final Relation input, final List<? extends Evaluation<?>> projectionList) {
    super(buildResultSchema(projectionList));
    if (input == null) {
      throw new IllegalArgumentException("Argument [input] is null.");
    }
    this.input = input;
    this.projectionList = Collections.unmodifiableList(projectionList);
    this.hashCode = 0;
  }

  private final Relation input;

  protected final List<? extends Evaluation<?>> projectionList;

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

  public List<? extends Evaluation<?>> getProjectionList() {
    return this.projectionList;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      int hashCode = this.getInput().hashCode();
      for (Evaluation projection : this.projectionList) {
        hashCode = 31 * hashCode + projection.hashCode();
      }
      this.hashCode = hashCode;
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
      final LogicalProject that = (LogicalProject) object;
      return this.getInput().equals(that.getInput()) && this.projectionList.equals(that.projectionList);
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("Select(");
    stringBuilder.append("input=").append(this.getInput().toString()).append("; ");
    stringBuilder.append("projectionList=[");
    for (int index = 0; index < this.projectionList.size(); index++) {
      if (index != 0) {
        stringBuilder.append(", ");
      }
      this.projectionList.get(index).toString(stringBuilder);
    }
    stringBuilder.append("])");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected LogicalProject copy(final Relation[] newInputList) {
    return new LogicalProject(newInputList[0], this.projectionList);
  }

  private static Table.Schema buildResultSchema(final List<? extends Evaluation<?>> projectionList) {
    final Table.SchemaBuilder schemaBuilder = SCHEMA_BUILDER_CACHE.get();
    schemaBuilder.reset();
    for (int index = 0; index < projectionList.size(); index++) {
      final Evaluation<?> projection = projectionList.get(index);
      schemaBuilder.addAttribute(
          projection.getResultClass(),
          projection.isNullable()
      );
    }
    return schemaBuilder.build();
  }

}
