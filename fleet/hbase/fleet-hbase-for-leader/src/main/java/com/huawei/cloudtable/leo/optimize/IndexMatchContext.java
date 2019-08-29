package com.huawei.cloudtable.leo.optimize;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.metadata.IndexDefinition;
import com.huawei.cloudtable.leo.metadata.IndexReference;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexMatchContext extends IndexColumnRangeParser.Context {

  public static IndexMatchContext get(final IndexReference indexReference, final Evaluation<Boolean> condition) {
    if (indexReference == null) {
      throw new IllegalArgumentException("Argument [indexReference] is null.");
    }
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    final IndexMatchContext context = CACHE.get();
    context.reset(indexReference, condition);
    return context;
  }

  private static final ThreadLocal<IndexMatchContext> CACHE = new ThreadLocal<IndexMatchContext>() {
    @Override
    protected IndexMatchContext initialValue() {
      return new IndexMatchContext();
    }
  };

  public IndexMatchContext() {
    this.indexColumnRangeMap = new HashMap<>();
    this.indexColumnIndexSet = new HashSet<>();
    this.indexColumnMapByExpression = new HashMap<>();
  }

  private IndexReference indexReference;

  private Evaluation<Boolean> condition;

  private final Map<Evaluation<?>, IndexColumnRange> indexColumnRangeMap;

  private final Set<Integer> indexColumnIndexSet;

  private final Map<Evaluation<?>, IndexDefinition.Column<?>> indexColumnMapByExpression;

  @Nonnull
  public Evaluation<Boolean> getCondition() {
    return this.condition;
  }

  @Nonnull
  public IndexReference getIndexReference() {
    return this.indexReference;
  }

  public IndexColumnRange getIndexColumnRange(final Evaluation<?> expression) {
    return this.indexColumnRangeMap.get(expression);
  }

  @Override
  IndexDefinition.Column<?> getIndexColumn(final Evaluation<?> expression) {
    final IndexDefinition.Column<?> indexColumn = this.indexColumnMapByExpression.get(expression);
    if (indexColumn == null) {
      return null;
    } else {
      this.indexColumnIndexSet.add(this.indexReference.getColumnIndex(indexColumn));
      return indexColumn;
    }
  }

  boolean isIndexColumnAppearedInCondition(final IndexDefinition.Column<?> indexColumn) {
    final Integer indexColumnIndex = this.indexReference.getColumnIndex(indexColumn);
    if (indexColumnIndex == null) {
      throw new RuntimeException();
    }
    return this.indexColumnIndexSet.contains(indexColumnIndex);
  }

  @Override
  void putResult(final Evaluation<?> expression, final IndexColumnRange indexColumnRange) {
    this.indexColumnRangeMap.put(expression, indexColumnRange);
  }

  private void reset(final IndexReference indexReference, final Evaluation<Boolean> condition) {
    this.indexReference = indexReference;
    this.condition = condition;
    this.indexColumnRangeMap.clear();
    this.indexColumnIndexSet.clear();
    this.indexColumnMapByExpression.clear();
    for (int index = 0; index < indexReference.getColumnCount(); index++) {
      final IndexReference.ColumnReference<?> indexColumnReference = indexReference.getColumnReference(index);
      this.indexColumnMapByExpression.put(indexColumnReference.getColumnExpression(), indexColumnReference.getColumn());
    }
  }

}
