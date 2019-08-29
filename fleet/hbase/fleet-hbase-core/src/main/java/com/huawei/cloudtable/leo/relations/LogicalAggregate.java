package com.huawei.cloudtable.leo.relations;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.Evaluation;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public final class LogicalAggregate extends Relation implements Relation.Logical {

  private static final ThreadLocal<Table.SchemaBuilder> SCHEMA_BUILDER_CACHE = new ThreadLocal<Table.SchemaBuilder>() {
    @Override
    protected Table.SchemaBuilder initialValue() {
      return new Table.SchemaBuilder();
    }
  };

  public LogicalAggregate(
      final Relation input,
      final List<? extends Aggregation<?>> aggregationList,
      final List<Evaluation<?>> groupBy,
      final GroupType groupType
  ) {
    super(buildResultSchema(aggregationList));
    if (input == null) {
      throw new IllegalArgumentException("Argument [input] is null.");
    }
    if (groupBy != null && groupBy.isEmpty()) {
      throw new IllegalArgumentException("Argument [groupBy] is empty.");
    }
    this.input = input;
    this.aggregationList = Collections.unmodifiableList(aggregationList);
    this.groupBy = groupBy == null ? null : Collections.unmodifiableList(groupBy);
    this.groupType = groupType;
    this.hashCode = 0;
  }

  private final Relation input;

  private final List<? extends Aggregation<?>> aggregationList;

  private final List<Evaluation<?>> groupBy;

  private final GroupType groupType;

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

  public List<? extends Aggregation<?>> getAggregationList() {
    return this.aggregationList;
  }

  public List<Evaluation<?>> getGroupBy() {
    return this.groupBy;
  }

  public GroupType getGroupType() {
    return this.groupType;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      int hashCode = 0;
      for (Aggregation aggregation : this.aggregationList) {
        hashCode = 31 * hashCode + aggregation.hashCode();
      }
      if (this.groupBy != null) {
        for (Evaluation evaluation : this.groupBy) {
          hashCode = 31 * hashCode + evaluation.hashCode();
        }
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
      final LogicalAggregate that = (LogicalAggregate) object;
      return this.aggregationList.equals(that.aggregationList)
          && (this.groupBy == null ? that.groupBy == null : this.groupBy.equals(that.groupBy));
    } else {
      return false;
    }
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    stringBuilder.append("Aggregate(");
    stringBuilder.append("input=").append(this.getInput(0).toString()).append("; ");
    stringBuilder.append("aggregationList=[");
    for (int index = 0; index < this.aggregationList.size(); index++) {
      if (index != 0) {
        stringBuilder.append(", ");
      }
      this.aggregationList.get(index).toString(stringBuilder);
    }
    stringBuilder.append("]");
    if (this.groupBy != null) {
      stringBuilder.append("; ");
      stringBuilder.append("groupBy=[");
      for (int expressionIndex = 0; expressionIndex < this.groupBy.size(); expressionIndex++) {
        final Evaluation<?> expression = this.groupBy.get(expressionIndex);
        if (expressionIndex != 0) {
          stringBuilder.append(", ");
        }
        expression.toString(stringBuilder);
      }
      stringBuilder.append("]");
    }
    stringBuilder.append(")");
  }

  @Override
  public <TReturn, TParameter> TReturn accept(final RelationVisitor<TReturn, TParameter> visitor, final TParameter parameter) {
    return visitor.visit(this, parameter);
  }

  @Override
  protected LogicalAggregate copy(final Relation[] newInputList) {
    return new LogicalAggregate(newInputList[0], this.aggregationList, this.groupBy, this.groupType);
  }

  private static Table.Schema buildResultSchema(final List<? extends Aggregation<?>> aggregationList) {
    final Table.SchemaBuilder schemaBuilder = SCHEMA_BUILDER_CACHE.get();
    schemaBuilder.reset();
    for (int index = 0; index < aggregationList.size(); index++) {
      final Aggregation<?> aggregation = aggregationList.get(index);
      schemaBuilder.addAttribute(
          aggregation.getResultClass(),
          aggregation.isNullable()
      );
    }
    return schemaBuilder.build();
  }

  public enum GroupType {

    ROLLUP,

    CUBE

  }

}
