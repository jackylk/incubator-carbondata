package com.huawei.cloudtable.leo.optimize;

import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.common.CollectionHelper;
import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.metadata.IndexReference;

import java.util.*;

public final class IndexPrefixMatcher {

  /**
   * 非最后一个Segment，都是单值Segment
   */
  public static List<IndexPrefixMatchResult> match(final IndexMatchContext context) {
    if (context == null) {
      throw new IllegalArgumentException("Argument [context] is null.");
    }
    IndexColumnRangeParser.parse(context, context.getCondition());
    final int maximumPrefixLength = computePrefixLength(context);
    if (maximumPrefixLength == 0) {
      return null;
    }
    final MatchKit matchKit = MatchKit.get();
    final ScanRangesParseContext scanRangesParseContext = matchKit.getScanRangesParseContext(context, maximumPrefixLength);
    // 列表中的区间为OR的关系
    final List<ScanRange> scanRangeList = ScanRangesParser.parse(scanRangesParseContext, context.getCondition());
    if (scanRangeList.isEmpty()) {
      return null;
    }
    // TODO 统一了前缀长度，可以优化为不统一
    return resolveMatchResult(context, matchKit, scanRangeList, computePrefixLength(context, scanRangeList));
  }

  /**
   * 计算理论上的前缀长度
   */
  private static int computePrefixLength(final IndexMatchContext context) {
    int prefixLength = 0;
    final IndexReference indexReference = context.getIndexReference();
    for (int index = 0; index < indexReference.getColumnCount(); index++) {
      if (context.isIndexColumnAppearedInCondition(indexReference.getColumn(index))) {
        prefixLength++;
      } else {
        break;
      }
    }
    return prefixLength;
  }

  /**
   * 计算实际上的前缀长度
   */
  private static int computePrefixLength(final IndexMatchContext context, final List<ScanRange> scanRangeList) {
    // TODO 这里可以判断组合出来的扫描区间个数是否超出了最大限制。
    final int dimensionCount = context.getIndexReference().getColumnCount() - 1;
    final int scanRangeCount = scanRangeList.size();
    int currentDimension = 0;
    for (; currentDimension < dimensionCount; currentDimension++) {
      int scanRangeIndex = 0;
      for (; scanRangeIndex < scanRangeCount; scanRangeIndex++) {
        final ValueRange indexColumnRange = scanRangeList.get(scanRangeIndex).get(currentDimension);
        final int intervalCount = indexColumnRange.getIntervalCount();
        int intervalIndex = 0;
        for (; intervalIndex < intervalCount; intervalIndex++) {
          if (!indexColumnRange.getInterval(intervalIndex).isPoint()) {
            break;
          }
        }
        if (intervalIndex < intervalCount) {
          break;
        }
      }
      if (scanRangeIndex < scanRangeCount) {
        // 并非所有的都是单值区间，匹配到此为止
        break;
      }
    }
    return currentDimension;
  }

  @SuppressWarnings("unchecked")
  private static List<IndexPrefixMatchResult> resolveMatchResult(
      final IndexMatchContext context,
      final MatchKit matchKit,
      final List<ScanRange> scanRangeList,
      final int prefixLength
  ) {
    // 排序，最后一列扫描区间的合并
    final Map<IndexPrefixMatchResult.Prefix, ValueRange> expandedScanRangeMap = matchKit.getExpandedScanRangeMap();
    if (prefixLength == 0) {
      ValueRange indexColumnRange = scanRangeList.get(0).get(0);
      for (int i = 1; i < scanRangeList.size(); i++) {
        indexColumnRange = indexColumnRange.or(scanRangeList.get(i).get(0));
      }
      if (indexColumnRange.isFull()) {
        // Full scan, is means not matched.
        return null;
      }
      expandedScanRangeMap.put(IndexPrefixMatchResult.Prefix.EMPTY, indexColumnRange);
    } else {
      final ValueRange[] indexColumnRangeVector = new ValueRange[prefixLength];
      final int[] intervalIndexMaximumList = new int[prefixLength];
      final int[] intervalIndexList = new int[prefixLength];
      for (ScanRange scanRange : scanRangeList) {
        for (int i = 0; i < prefixLength; i++) {
          intervalIndexMaximumList[i] = scanRange.get(i).getIntervalCount() - 1;
        }
        int dimensionIndex = 0;
        while (true) {
          if (dimensionIndex == prefixLength) {
            final Comparable[] prefixValues = new Comparable[prefixLength];
            for (int index = 0; index < prefixLength; index++) {
              prefixValues[index] = indexColumnRangeVector[index].getInterval(0).getMinimum();
            }
            final IndexPrefixMatchResult.Prefix prefix = new IndexPrefixMatchResult.Prefix(prefixValues);
            // Merge value range.
            final ValueRange oldRange = expandedScanRangeMap.get(prefix);
            final ValueRange newRange = scanRange.get(dimensionIndex);
            if (oldRange == null) {
              expandedScanRangeMap.put(prefix, newRange);
            } else {
              expandedScanRangeMap.put(prefix, oldRange.or(newRange));
            }
            dimensionIndex--;
          }
          final int intervalIndex = intervalIndexList[dimensionIndex];
          if (intervalIndex <= intervalIndexMaximumList[dimensionIndex]) {
            indexColumnRangeVector[dimensionIndex] = ValueRange.of(scanRange.get(dimensionIndex).getInterval(intervalIndex));
            intervalIndexList[dimensionIndex] = intervalIndex + 1;
            dimensionIndex++;
          } else {
            // 不需要重置indexColumnRangeVector中的元素
            intervalIndexList[dimensionIndex] = 0;
            dimensionIndex--;
            if (dimensionIndex < 0) {
              break;
            }
          }
        }
      }
    }
    if (expandedScanRangeMap.isEmpty()) {
      return null;
    }
    // 展开, 对过程条件进行重构
    final List<IndexPrefixMatchResult> resultList = new ArrayList<>(expandedScanRangeMap.size());
    for (Map.Entry<IndexPrefixMatchResult.Prefix, ValueRange> entry : expandedScanRangeMap.entrySet()) {
      final IndexPrefixMatchResult.Prefix prefix = entry.getKey();
      final ValueRange scanRange = entry.getValue();
      for (int index = 0; index < scanRange.getIntervalCount(); index++) {
        final IndexPrefixMatchResult result = new IndexPrefixMatchResult(prefix, scanRange.getInterval(index));
        // 重构过滤条件
        final ConditionReduceContext conditionReduceContext = matchKit.getConditionReduceContext(context, result);
        final Evaluation<Boolean> reducedCondition = ConditionReducer.reduce(conditionReduceContext, context.getCondition());
        if (reducedCondition == ConditionReducer.BOOLEAN_FALSE) {
          continue;
        }
        if (reducedCondition != ConditionReducer.BOOLEAN_TRUE) {
          result.setFilterCondition(reducedCondition);
        }
        resultList.add(result);
      }
    }
    Collections.sort(resultList, ResultComparator.INSTANCE);
    return resultList;
  }

  private static final class MatchKit {

    static MatchKit get() {
      return CACHE.get();
    }

    private static final ThreadLocal<MatchKit> CACHE = new ThreadLocal<MatchKit>() {
      @Override
      protected MatchKit initialValue() {
        return new MatchKit();
      }
    };

    private MatchKit() {
      this.expandedScanRangeMap = new HashMap<>();
      this.scanRangesParseContext = new ScanRangesParseContext();
      this.conditionReduceContext = new ConditionReduceContext();
    }

    private final Map<IndexPrefixMatchResult.Prefix, ValueRange> expandedScanRangeMap;

    private final ScanRangesParseContext scanRangesParseContext;

    private final ConditionReduceContext conditionReduceContext;

    public Map<IndexPrefixMatchResult.Prefix, ValueRange> getExpandedScanRangeMap() {
      this.expandedScanRangeMap.clear();
      return this.expandedScanRangeMap;
    }

    public ScanRangesParseContext getScanRangesParseContext(final IndexMatchContext indexMatchContext, final int maximumPrefixLength) {
      this.scanRangesParseContext.reset(indexMatchContext, maximumPrefixLength);
      return this.scanRangesParseContext;
    }

    public ConditionReduceContext getConditionReduceContext(final IndexMatchContext indexMatchContext, final IndexPrefixMatchResult matchResult) {
      this.conditionReduceContext.reset(indexMatchContext, matchResult);
      return this.conditionReduceContext;
    }

  }

  private static final class ResultComparator implements Comparator<IndexPrefixMatchResult> {

    static final ResultComparator INSTANCE = new ResultComparator();

    @SuppressWarnings("unchecked")
    @Override
    public int compare(final IndexPrefixMatchResult result1, final IndexPrefixMatchResult result2) {
      final IndexPrefixMatchResult.Prefix prefix1 = result1.getPrefix();
      final IndexPrefixMatchResult.Prefix prefix2 = result2.getPrefix();
      final int dimension = prefix1.getDimension();
      for (int dimensionIndex = 0; dimensionIndex < dimension; dimensionIndex++) {
        final Comparable value1 = prefix1.get(dimensionIndex);
        final Comparable value2 = prefix2.get(dimensionIndex);
        final int compareResult = value1.compareTo(value2);
        if (compareResult != 0) {
          return compareResult;
        }
      }
      return 0;
    }

  }

  private static final class ScanRange {

    private static List<ScanRange> newFullScanRangeList(final int dimension) {
      if (dimension <= CACHE_SIZE) {
        return FULL_SCAN_RANGE_CACHE.get(dimension);
      } else {
        final ValueRange[] indexColumnRangeVector = new ValueRange[dimension];
        Arrays.fill(indexColumnRangeVector, ValueRange.full());
        return Collections.singletonList(new ScanRange(indexColumnRangeVector));
      }
    }

    static ValueRange[] newIndexColumnRangeVector(final int dimension) {
      if (dimension <= CACHE_SIZE) {
        return INDEX_COLUMN_RANGE_VECTOR_TEMPLATE_CACHE.get(dimension).clone();
      } else {
        final ValueRange[] indexColumnRangeVector = new ValueRange[dimension];
        Arrays.fill(indexColumnRangeVector, ValueRange.full());
        return indexColumnRangeVector;
      }
    }

    private static final ScanRange EMPTY = new ScanRange(new ValueRange[0]);

    private static final int CACHE_SIZE = 16;

    private static final Map<Integer, List<ScanRange>> FULL_SCAN_RANGE_CACHE = new HashMap<>();

    private static final Map<Integer, ValueRange[]> INDEX_COLUMN_RANGE_VECTOR_TEMPLATE_CACHE = new HashMap<>();

    static {
      for (int dimension = 1; dimension <= CACHE_SIZE; dimension++) {
        final ValueRange[] indexColumnRangeVector = new ValueRange[dimension];
        Arrays.fill(indexColumnRangeVector, ValueRange.full());
        FULL_SCAN_RANGE_CACHE.put(dimension, Collections.unmodifiableList(Collections.singletonList(new ScanRange(indexColumnRangeVector))));
        INDEX_COLUMN_RANGE_VECTOR_TEMPLATE_CACHE.put(dimension, indexColumnRangeVector);
      }
    }

    ScanRange(final ValueRange[] indexColumnRangeVector) {
      this.indexColumnRangeVector = indexColumnRangeVector;
    }

    private ValueRange[] indexColumnRangeVector;

    public int getDimension() {
      return this.indexColumnRangeVector.length;
    }

    public ValueRange get(final int dimensionIndex) {
      return this.indexColumnRangeVector[dimensionIndex];
    }

    void setLast(final ValueRange indexColumnRange) {
      CollectionHelper.setLast(this.indexColumnRangeVector, indexColumnRange);
    }

    List<ScanRange> not() {
      final List<ScanRange> result = new ArrayList<>();
      this.not0(0, newIndexColumnRangeVector(this.getDimension()), result);
      return result;
    }

    void not(final List<ScanRange> result) {
      this.not0(0, newIndexColumnRangeVector(this.getDimension()), result);
    }

    private void not0(final int dimensionIndex, final ValueRange[] template, final List<ScanRange> result) {
      final ValueRange range = this.get(dimensionIndex);
      if (dimensionIndex == this.getDimension() - 1) {
        // The last range.
        if (!range.isFull()) {
          template[dimensionIndex] = range.not();
          result.add(new ScanRange(template));
        }
      } else {
        if (!range.isFull()) {
          final ValueRange[] newRangeVector = template.clone();
          newRangeVector[dimensionIndex] = range.not();
          result.add(new ScanRange(newRangeVector));
        }
        try {
          template[dimensionIndex] = range;
          this.not0(dimensionIndex + 1, template, result);
        } finally {
          template[dimensionIndex] = ValueRange.full();
        }
      }
    }

    @SuppressWarnings("unchecked")
    ScanRange and(final ScanRange that) {
      // 参数scanInterval1和scanInterval2的长度必须相同，对应位置的Interval的数据类型与必须相同
      if (this.getDimension() == 0) {
        return EMPTY;
      }
      ValueRange[] newRangeVector = new ValueRange[this.getDimension()];
      for (int i = 0; i < this.getDimension(); i++) {
        final ValueRange newRange = this.get(i).and(that.get(i));
        if (newRange.getIntervalCount() == 0) {
          return null;
        } else {
          newRangeVector[i] = newRange;
        }
      }
      return new ScanRange(newRangeVector);
    }
  }

  private static final class ScanRangesParser implements LogicalVisitor<List<ScanRange>, ScanRangesParseContext> {

    private static final ScanRangesParser INSTANCE = new ScanRangesParser();

    private ScanRangesParser() {
      // to do nothing.
    }

    static List<ScanRange> parse(final ScanRangesParseContext context, final Evaluation<Boolean> condition) {
      if (condition instanceof Logical) {
        return ((Logical) condition).accept(INSTANCE, context);
      }
      final IndexColumnRange indexColumnRange = context.getIndexColumnInterval(condition);
      if (indexColumnRange == null) {
        return ScanRange.newFullScanRangeList(context.getMaximumPrefixLength() + 1);
      }
      final Integer indexColumnIndex = context.getIndexReference().getColumnIndex(indexColumnRange.getIndexColumn());
      if (indexColumnIndex == null || indexColumnIndex >= context.getMaximumPrefixLength()) {
        return ScanRange.newFullScanRangeList(context.getMaximumPrefixLength() + 1);
      }
      final ValueRange[] indexColumnRangeVector = ScanRange.newIndexColumnRangeVector(context.getMaximumPrefixLength() + 1);
      indexColumnRangeVector[indexColumnIndex] = indexColumnRange.getRange();
      return Collections.singletonList(new ScanRange(indexColumnRangeVector));
    }

    @Override
    public List<ScanRange> visit(final LogicalNot not, final ScanRangesParseContext context) {
      return ScanRangesHelper.not(parse(context, not.getParameter()), context.getMaximumPrefixLength());
    }

    @Override
    public List<ScanRange> visit(final LogicalAnd and, final ScanRangesParseContext context) {
      final List<ScanRange> result = new ArrayList<>();
      ScanRangesHelper.and(parse(context, and.getParameter1()), parse(context, and.getParameter2()), result);
      return result;
    }

    @Override
    public List<ScanRange> visit(final LogicalOr or, final ScanRangesParseContext context) {
      final List<ScanRange> result = new ArrayList<>();
      result.addAll(parse(context, or.getParameter1()));
      result.addAll(parse(context, or.getParameter2()));
      return result;
    }

    @Override
    public List<ScanRange> visit(final LogicalXor xor, final ScanRangesParseContext context) {
      // TODO
      throw new UnsupportedOperationException();
    }

  }

  private static final class ScanRangesParseContext {

    ScanRangesParseContext() {
      // to do nothing.
    }

    private IndexMatchContext indexMatchContext;

    private int maximumPrefixLength;

    void reset(final IndexMatchContext indexMatchContext, final int maximumPrefixLength) {
      this.indexMatchContext = indexMatchContext;
      this.maximumPrefixLength = maximumPrefixLength;
    }

    IndexReference getIndexReference() {
      return this.indexMatchContext.getIndexReference();
    }

    IndexColumnRange getIndexColumnInterval(final Evaluation<Boolean> condition) {
      return this.indexMatchContext.getIndexColumnRange(condition);
    }

    int getMaximumPrefixLength() {
      return this.maximumPrefixLength;
    }

  }

  private static final class ScanRangesHelper {

    static List<ScanRange> not(final List<ScanRange> scanRangeList, final int indexColumnCount) {
      switch (scanRangeList.size()) {
        case 0:
          return Collections.singletonList(new ScanRange(ScanRange.newIndexColumnRangeVector(indexColumnCount)));
        case 1:
          return scanRangeList.get(0).not();
        default:
          List<ScanRange> scanRangeList1 = scanRangeList.get(0).not();
          List<ScanRange> scanRangeList2 = scanRangeList.get(1).not();
          List<ScanRange> result = new ArrayList<>();
          and(scanRangeList1, scanRangeList2, result);
          for (int i = 2; i < scanRangeList.size(); i++) {
            scanRangeList1.clear();
            scanRangeList2.clear();
            scanRangeList.get(i).not(scanRangeList1);
            and(scanRangeList1, result, scanRangeList2);
            // Exchange result and scanRangeList2.
            final List<ScanRange> temporary = result;
            result = scanRangeList2;
            scanRangeList2 = temporary;
          }
          return result;
      }
    }

    static void and(final List<ScanRange> scanRangeList1, final List<ScanRange> scanRangeList2, final List<ScanRange> result) {
      if (scanRangeList1.isEmpty() || scanRangeList2.isEmpty()) {
        return;
      }
      for (ScanRange scanRange1 : scanRangeList1) {
        for (ScanRange scanRange2 : scanRangeList2) {
          final ScanRange scanRange = scanRange1.and(scanRange2);
          if (scanRange != null) {
            result.add(scanRange);
          }
        }
      }
    }

  }

  private static final class ConditionReducer implements LogicalVisitor<Evaluation<Boolean>, ConditionReduceContext> {

    private static final Constant<Boolean> BOOLEAN_TRUE = new Constant<>(Boolean.class, true);

    private static final Constant<Boolean> BOOLEAN_FALSE = new Constant<>(Boolean.class, false);

    private static final ConditionReducer INSTANCE = new ConditionReducer();

    @SuppressWarnings("unchecked")
    static Evaluation<Boolean> reduce(final ConditionReduceContext context, final Evaluation<Boolean> condition) {
      if (condition instanceof Logical) {
        return ((Logical) condition).accept(INSTANCE, context);
      }
      final IndexColumnRange indexColumnRange = context.getIndexColumnInterval(condition);
      if (indexColumnRange == null) {
        return condition;
      }
      final Integer indexColumnIndex = context.getIndexReference().getColumnIndex(indexColumnRange.getIndexColumn());
      if (indexColumnIndex == null) {
        return condition;
      }
      final IndexPrefixMatchResult matchResult = context.getMatchResult();
      final IndexPrefixMatchResult.Prefix matchResultPrefix = matchResult.getPrefix();
      if (indexColumnIndex > matchResultPrefix.getDimension()) {
        return condition;
      }
      final ValueRange valueRange = indexColumnRange.getRange();
      if (indexColumnIndex == matchResultPrefix.getDimension()) {
        final ValueRange.Interval scanInterval = matchResult.getScanInterval();
        switch (valueRange.contains(scanInterval)) {
          case COMPLETELY:
            return BOOLEAN_TRUE;
          case PARTIALLY:
            return condition;
          case NOT_CONTAINS:
            return BOOLEAN_FALSE;
          default:
            throw new RuntimeException();
        }
      } else {
        final Comparable value = matchResult.getPrefix().get(indexColumnIndex);
        if (valueRange.contains(value)) {
          if (valueRange.isPoint()) {
            return BOOLEAN_TRUE;
          } else {
            return condition;
          }
        } else {
          return BOOLEAN_FALSE;
        }
      }
    }

    private ConditionReducer() {
      // to do nothing.
    }

    @Override
    public Evaluation<Boolean> visit(final LogicalNot expression, final ConditionReduceContext context) {
      final Evaluation<Boolean> parameter = expression.getParameter();
      final Evaluation<Boolean> result = reduce(context, parameter);
      if (result == BOOLEAN_FALSE) {
        return BOOLEAN_TRUE;
      }
      if (result == BOOLEAN_TRUE) {
        return BOOLEAN_FALSE;
      }
      if (result != parameter) {
        return new LogicalNot(parameter);
      } else {
        return expression;
      }
    }

    @Override
    public Evaluation<Boolean> visit(final LogicalAnd expression, final ConditionReduceContext context) {
      final Evaluation<Boolean> parameter1 = expression.getParameter1();
      final Evaluation<Boolean> parameter2 = expression.getParameter2();
      final Evaluation<Boolean> newParameter1 = reduce(context, parameter1);
      if (newParameter1 == BOOLEAN_FALSE) {
        return BOOLEAN_FALSE;
      }
      final Evaluation<Boolean> newParameter2 = reduce(context, parameter2);
      if (newParameter2 == BOOLEAN_FALSE) {
        return BOOLEAN_FALSE;
      }
      if (newParameter1 == BOOLEAN_TRUE) {
        return newParameter2;
      }
      if (newParameter2 == BOOLEAN_TRUE) {
        return newParameter1;
      }
      if (newParameter1 == parameter1 && newParameter2 == parameter2) {
        return expression;
      } else {
        return new LogicalAnd(newParameter1, newParameter2);
      }
    }

    @Override
    public Evaluation<Boolean> visit(final LogicalOr expression, final ConditionReduceContext context) {
      final Evaluation<Boolean> parameter1 = expression.getParameter1();
      final Evaluation<Boolean> parameter2 = expression.getParameter2();
      final Evaluation<Boolean> newParameter1 = reduce(context, parameter1);
      if (newParameter1 == BOOLEAN_TRUE) {
        return BOOLEAN_TRUE;
      }
      final Evaluation<Boolean> newParameter2 = reduce(context, parameter2);
      if (newParameter2 == BOOLEAN_TRUE) {
        return BOOLEAN_TRUE;
      }
      if (newParameter1 == BOOLEAN_FALSE) {
        return newParameter2;
      }
      if (newParameter2 == BOOLEAN_FALSE) {
        return newParameter1;
      }
      if (newParameter1 == parameter1 && newParameter2 == parameter2) {
        return expression;
      } else {
        return new LogicalOr(newParameter1, newParameter2);
      }
    }

    @Override
    public Evaluation<Boolean> visit(final LogicalXor expression, final ConditionReduceContext context) {
      final Evaluation<Boolean> parameter1 = expression.getParameter1();
      final Evaluation<Boolean> parameter2 = expression.getParameter2();
      final Evaluation<Boolean> newParameter1 = reduce(context, parameter1);
      final Evaluation<Boolean> newParameter2 = reduce(context, parameter2);
      if (newParameter1 == BOOLEAN_TRUE) {
        if (newParameter2 == BOOLEAN_TRUE) {
          return BOOLEAN_FALSE;
        }
        if (newParameter2 == BOOLEAN_FALSE) {
          return BOOLEAN_TRUE;
        }
      }
      if (newParameter1 == BOOLEAN_FALSE) {
        if (newParameter2 == BOOLEAN_TRUE) {
          return BOOLEAN_TRUE;
        }
        if (newParameter2 == BOOLEAN_FALSE) {
          return BOOLEAN_FALSE;
        }
      }
      if (newParameter1 == parameter1 && newParameter2 == parameter2) {
        return expression;
      } else {
        return new LogicalXor(newParameter1, newParameter2);
      }
    }

  }

  private static final class ConditionReduceContext {

    ConditionReduceContext() {
      // to do nothing.
    }

    private IndexMatchContext indexMatchContext;

    private IndexPrefixMatchResult matchResult;

    void reset(final IndexMatchContext indexMatchContext, final IndexPrefixMatchResult matchResult) {
      this.indexMatchContext = indexMatchContext;
      this.matchResult = matchResult;
    }

    IndexReference getIndexReference() {
      return this.indexMatchContext.getIndexReference();
    }

    IndexColumnRange getIndexColumnInterval(final Evaluation<Boolean> condition) {
      return this.indexMatchContext.getIndexColumnRange(condition);
    }

    /**
     * 假设的当前扫描区间。
     */
    IndexPrefixMatchResult getMatchResult() {
      return this.matchResult;
    }

  }

}
