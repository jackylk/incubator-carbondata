package com.huawei.cloudtable.leo.planners;

import com.huawei.cloudtable.leo.ExecuteContext;
import com.huawei.cloudtable.leo.ExecuteException;
import com.huawei.cloudtable.leo.ExecuteParameters;
import com.huawei.cloudtable.leo.ExecutePlan;
import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.HBaseExecuteEngine;
import com.huawei.cloudtable.leo.HBaseExecutePlanner;
import com.huawei.cloudtable.leo.HBaseRowKeyCodec;
import com.huawei.cloudtable.leo.HBaseRowKeyHelper;
import com.huawei.cloudtable.leo.HBaseRowKeyValueIterator;
import com.huawei.cloudtable.leo.HBaseTableColumnReference;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.RelationVisitor;
import com.huawei.cloudtable.leo.Table;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.ValueRange;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.hbase.filters.ConditionFilter;
import com.huawei.cloudtable.leo.language.expression.SQLNull;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.optimize.IndexMatchContext;
import com.huawei.cloudtable.leo.optimize.IndexPrefixMatchResult;
import com.huawei.cloudtable.leo.optimize.IndexPrefixMatcher;
import com.huawei.cloudtable.leo.relations.LogicalAggregate;
import com.huawei.cloudtable.leo.relations.LogicalFilter;
import com.huawei.cloudtable.leo.relations.LogicalLimit;
import com.huawei.cloudtable.leo.relations.LogicalOffset;
import com.huawei.cloudtable.leo.relations.LogicalProject;
import com.huawei.cloudtable.leo.relations.LogicalScan;
import com.huawei.cloudtable.leo.statements.SelectStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class SelectPlanner extends HBaseExecutePlanner<Table, SelectStatement> {
  private static final Log LOG = LogFactory.getLog(SelectPlanner.class.getName());

  public SelectPlanner() {
    super(SelectStatement.class);
  }

  @Override
  public ExecutePlan<Table> plan(
      final HBaseExecuteEngine executeEngine,
      final ExecuteContext executeContext,
      final SelectStatement statement
  ) {
    final CompileContext compileContext = CompileContext.get(statement.getHints());
    try {
      return new SelectPlan(
          ResultIteratorGenerator.generate(
              executeEngine,
              executeContext,
              compileContext,
              statement.getRelation()
          )
      );
    } finally {
      compileContext.reset();
    }
  }

  private static boolean isGet(final IndexPrefixMatchResult primaryKeyMatchResult, final TableDefinition.PrimaryKey primaryKey) {
    return primaryKeyMatchResult.getPrefix().getDimension() == primaryKey.getColumnCount() - 1
        && primaryKeyMatchResult.getScanInterval().isPoint();
  }

  private static final class SelectPlan extends ExecutePlan<Table> {

    SelectPlan(final ResultIterator resultIterator) {
      this.resultIterator = resultIterator;
      this.resultIteratorUsed = false;
    }

    private final ResultIterator resultIterator;

    private boolean resultIteratorUsed;

    @Override
    public Table execute() {
      if (this.resultIteratorUsed) {
        return this.resultIterator.duplicate();
      } else {
        this.resultIteratorUsed = true;
        return this.resultIterator;
      }
    }

    @Override
    public Table execute(final ExecuteParameters parameters) {
      return this.resultIterator.duplicate(parameters);
    }

  }

  private static final class CompileContext implements Evaluation.CompileContext {

    static CompileContext get(final Set<String> hints) {
      final CompileContext context = CACHE.get();
      context.reset(hints);
      return context;
    }

    private static final ThreadLocal<CompileContext> CACHE = new ThreadLocal<CompileContext>() {
      @Override
      protected CompileContext initialValue() {
        return new CompileContext();
      }
    };

    private CompileContext() {
      // to do nothing.
    }

    private Set<String> hints;

    @Override
    public Charset getCharset() {
      // TODO
      return Charset.forName("UTF-8");
    }

    @Override
    public <TValue> ValueType<TValue> getValueType(final Class<TValue> valueClass) {
      // TODO
      return ValueTypeManager.BUILD_IN.getValueType(valueClass);
    }

    @Override
    public <TValue> ValueCodec<TValue> getValueCodec(final Class<TValue> valueClass) {
      return HBaseValueCodecManager.BUILD_IN.getValueCodec(valueClass);
    }

    @Override
    public boolean hasHint(final String hintName) {
      return this.hints.contains(hintName);
    }

    private void reset(final Set<String> hints) {
      this.hints = hints;
    }

    void reset() {
      this.reset(null);
    }

  }

  private static final class RuntimeContext implements Evaluation.RuntimeContext {

    RuntimeContext(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final ResultIterator input
    ) {
      this.executeEngine = executeEngine;
      this.executeParameters = executeParameters;
      this.input = input;
    }

    private final HBaseExecuteEngine executeEngine;

    private final ExecuteParameters executeParameters;

    private final ResultIterator input;

    @Override
    public <TValue> TValue getVariable(final Variable<TValue> variable) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> ValueBytes getVariableAsBytes(final Variable<TValue> variable, final ValueCodec<TValue> valueCodec) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <TValue> TValue getAttribute(final Reference<TValue> reference) {
      return (TValue) this.input.get(reference.getAttributeIndex());
    }

    @Override
    public <TValue> ValueBytes getAttributeAsBytes(final Reference<TValue> reference, final ValueCodec<TValue> valueCodec) {
      return this.input.get(reference.getAttributeIndex(), valueCodec);
    }

  }

  private static final class ResultsScanner implements ResultScanner {

    ResultsScanner(final Result[] results) {
      this.results = results;
      this.resultIndex = 0;
    }

    private final Result[] results;

    private int resultIndex;

    @Override
    public Result next() {
      while (this.resultIndex < this.results.length) {
        final Result result = this.results[this.resultIndex];
        this.resultIndex++;
        if (result != null) {
          return result;
        }
      }
      return null;
    }

    @Override
    public Result[] next(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean renewLease() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScanMetrics getScanMetrics() {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Result> iterator() {
      throw new UnsupportedOperationException();
    }

  }

  static abstract class ResultIterator extends Table {

    ResultIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final Schema schema
    ) {
      super(schema);
      this.executeEngine = executeEngine;
      this.executeParameters = executeParameters;
    }

    ResultIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final Schema schema,
        final List<Order> orderBy
    ) {
      super(schema, orderBy);
      this.executeEngine = executeEngine;
      this.executeParameters = executeParameters;
    }

    final HBaseExecuteEngine executeEngine;

    final ExecuteParameters executeParameters;

    @Override
    public <TValue> TValue get(final int attributeIndex, final Class<TValue> resultClass) {
      // TODO 通过隐式转换器转换
      return resultClass.cast(this.get(attributeIndex));
    }

    public abstract ValueBytes get(int attributeIndex, ValueCodec resultCodec);

    ResultIterator duplicate() {
      return this.duplicate(null);
    }

    abstract ResultIterator duplicate(ExecuteParameters executeParameters);

  }

  private static final class ResultIteratorGenerator extends RelationVisitor<ResultIterator, ResultIteratorGenerator.Context> {

    static final class Context {

      Context() {
        // to do nothing.
      }

      private HBaseExecuteEngine executeEngine;

      private ExecuteContext executeContext;

      private Evaluation.CompileContext compileContext;

      void reset(
          final HBaseExecuteEngine executeEngine,
          final ExecuteContext executeContext,
          final Evaluation.CompileContext compileContext
      ) {
        this.executeEngine = executeEngine;
        this.executeContext = executeContext;
        this.compileContext = compileContext;
      }

      HBaseExecuteEngine getExecuteEngine() {
        return this.executeEngine;
      }

      ExecuteContext getExecuteContext() {
        return this.executeContext;
      }

      public Evaluation.CompileContext getCompileContext() {
        return this.compileContext;
      }

    }

    static ResultIterator generate(
        final HBaseExecuteEngine executeEngine,
        final ExecuteContext executeContext,
        final Evaluation.CompileContext compileContext,
        final Relation relation
    ) {
      final Context context = CONTEXT_CACHE.get();
      context.reset(executeEngine, executeContext, compileContext);
      try {
        return relation.accept(INSTANCE, context);
      } finally {
        context.reset(null, null, null);
      }
    }

    private static final ResultIteratorGenerator INSTANCE = new ResultIteratorGenerator();

    private static final ThreadLocal<Context> CONTEXT_CACHE = new ThreadLocal<Context>() {
      @Override
      protected Context initialValue() {
        return new Context();
      }
    };

    private ResultIteratorGenerator() {
      // to do nothing.
    }

    @Override
    public ResultIterator returnDefault(final Relation relation, final Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultIterator visit(final LogicalAggregate aggregate, final Context context) {
      HBaseTableReference tableReference;
      if (aggregate.getInput() instanceof LogicalFilter) {
        final LogicalFilter filter = aggregate.getInput();
        if (filter.getInput() instanceof LogicalScan) {
          final LogicalScan scan = filter.getInput();
          if (!(scan.getTableReference() instanceof HBaseTableReference)) {
            throw new IllegalArgumentException();
          } else {
            tableReference = (HBaseTableReference) scan.getTableReference();
          }
          // 由于SQL解析后传入的condition和groupby的列信息，非原始数据的列信息，因此，需要做一次类型转换
          // 例如原始表有5列，其中对于原始表的低4列做过滤。在SQL解析后传入的列序号，为0，而真实的原始数据为4
          // 因此，需要做此类转换
          final Evaluation<Boolean> condition = (Evaluation<Boolean>) reconstructCondition(context, filter.getCondition(), scan);
          final List<Evaluation<?>> reconstructedGroupByList = reconstructGroupBy(aggregate, context, scan);
          final List<? extends Aggregation<?>> reconstructedAggregationList = reconstructAggregations(aggregate, context, scan);

          final IndexMatchContext indexMatchContext = IndexMatchContext.get(tableReference.getPrimaryKeyIndex(),
              condition);
          final List<IndexPrefixMatchResult> indexMatchResultList = IndexPrefixMatcher.match(indexMatchContext);

          try {
            return new ResultAggregateIterator(context.getExecuteEngine(), tableReference, null,
              aggregate.getResultSchema(), indexMatchResultList, reconstructedAggregationList,
              reconstructedGroupByList);
          } catch (Exception e) {
            LOG.error(e);
          }
        }
      }
      return null;
    }

    @Override
    public ResultIterator visit(final LogicalFilter filter, final Context context) {
      if (filter.getInput() instanceof LogicalScan) {
        final LogicalScan scan = (LogicalScan) filter.getInput();
        if (!(scan.getTableReference() instanceof HBaseTableReference)) {
          throw new RuntimeException();
        }
        // Reconstruct condition.
        final Evaluation<Boolean> condition = (Evaluation<Boolean>) reconstructCondition(context, filter.getCondition(), scan);
        //
        final HBaseTableReference tableReference = (HBaseTableReference) scan.getTableReference();
        if (tableReference.getPrimaryKey() != null) {
          final IndexMatchContext indexMatchContext = IndexMatchContext.get(tableReference.getPrimaryKeyIndex(), condition);
          final List<IndexPrefixMatchResult> indexMatchResultList = IndexPrefixMatcher.match(indexMatchContext);
          if (indexMatchResultList != null) {
            if (indexMatchResultList.size() == 1 && isGet(indexMatchResultList.get(0), tableReference.getPrimaryKey())) {
              // TODO Filter Condition.
              // Single get.
              return new ResultGetIterator(
                  context.getExecuteEngine(),
                  null,
                  tableReference,
                  scan.getTableColumnReferenceList(),
                  indexMatchResultList.get(0),
                  filter.getResultSchema()
              );
            } else {
              // TODO Filter Condition.
              // Interval scan.
              return new ResultScanIterator(
                  context.getExecuteEngine(),
                  null,
                  tableReference,
                  scan.getTableColumnReferenceList(),
                  indexMatchResultList,
                  scan.getResultSchema()
              );
            }
          }
        }
        // TODO 判断是否允许在RegionServer端进行过滤，如过滤条件是否使用UDF和UDT，Hint提示等。
        return new ResultFullScanIterator(
            context.getExecuteEngine(),
            null,
            tableReference,
            scan.getTableColumnReferenceList(),
            scan.getResultSchema(),
            condition
        );
      }
      filter.getCondition().compile(context.getCompileContext());
      return new ResultFilterIterator(
          context.getExecuteEngine(),
          null,
          filter.getInput().accept(this, context),
          filter.getCondition()
      );
    }

    @Override
    public ResultIterator visit(final LogicalScan scan, final Context context) {
      if (!(scan.getTableReference() instanceof HBaseTableReference)) {
        throw new RuntimeException();
      }
      final HBaseTableReference tableReference = (HBaseTableReference) scan.getTableReference();
      return new ResultFullScanIterator(
          context.getExecuteEngine(),
          null,
          tableReference,
          scan.getTableColumnReferenceList(),
          scan.getResultSchema()
      );
    }

    @Override
    public ResultIterator visit(final LogicalLimit limit, final Context context) {
      return new ResultLimitIterator(
        context.getExecuteEngine(),
        null,
        limit.getInput().accept(this, context),
        limit.getInput().getResultSchema(),
        limit.getLimit()
      );
    }

    @Override
    public ResultIterator visit(final LogicalOffset offset, final Context context) {
      return super.visit(offset, context);
    }

    @Override
    public ResultIterator visit(final LogicalProject project, final Context context) {
      final List<? extends Evaluation<?>> projectionList = project.getProjectionList();
      for (int index = 0; index < projectionList.size(); index++) {
        projectionList.get(index).compile(context.getCompileContext());
      }
      return new ResultProjectIterator(
          context.getExecuteEngine(),
          null,
          project.getInput().accept(this, context),
          project.getResultSchema(),
          project.getProjectionList()
      );
    }

    private List<Evaluation<?>> reconstructGroupBy(LogicalAggregate aggregate, Context context, LogicalScan scan) {
      final List<Evaluation<?>> groupByList = aggregate.getGroupBy();
      final List<Evaluation<?>> reconstructedGroupByList;
      if (groupByList != null && groupByList.size() > 0) {
        reconstructedGroupByList = new ArrayList<>(groupByList.size());
        for (Evaluation<?> evaluation : groupByList) {
          reconstructedGroupByList.add((Evaluation<?>) reconstructCondition(context, evaluation, scan));
        }
      } else {
        reconstructedGroupByList = new ArrayList<>(0);
      }
      return reconstructedGroupByList;
    }

    private List<? extends Aggregation<?>> reconstructAggregations(LogicalAggregate aggregate, Context context, LogicalScan scan) {
      final List<? extends Aggregation<?>> aggregationList = aggregate.getAggregationList();
      final List<Aggregation<?>> reconstructedAggregationList;
      if (aggregationList != null && aggregationList.size() > 0) {
        reconstructedAggregationList = new ArrayList<>(aggregationList.size());
        for (Aggregation<?> aggregation : aggregationList) {
          final Aggregation<?> reconstructedAggregation = (Aggregation<?>) reconstructCondition(context, aggregation, scan);
          reconstructedAggregationList.add(reconstructedAggregation);
        }
      } else {
        reconstructedAggregationList = new ArrayList<>(0);
      }
      return reconstructedAggregationList;
    }

    private <TResult> Expression<TResult> reconstructCondition(Context context, Expression<TResult> expression, LogicalScan scan) {
      // Reconstruct condition.
      final ExpressionReconstructor.Context expressionReconstructContext = ExpressionReconstructor.Context.get();
      final Expression<TResult> condition;
      try {
        try {
          expressionReconstructContext.reset(context.getExecuteEngine().getFunctionManager(context.getExecuteContext()),
              scan.getTableColumnReferenceList());
        } catch (ExecuteException exception) {
          throw new UndeclaredThrowableException(exception);
        }
        condition = ExpressionReconstructor.INSTANCE.reconstruct(expressionReconstructContext, expression);
      } finally {
        expressionReconstructContext.reset(null, null);
      }
      return condition;
    }

  }

  private static final class ResultLimitIterator extends ResultIterator {
    ResultLimitIterator(
      final HBaseExecuteEngine executeEngine,
      final ExecuteParameters executeParameters,
      final ResultIterator input,
      final Schema schema,
      final long limit) {
      super(executeEngine, executeParameters, schema);
      this.input = input;
      this.limit = limit;
      this.remain = limit;
    }

    private final ResultIterator input;

    private final long limit;

    private long remain;

    @Override
    public boolean next() throws IOException {
      if (this.remain == 0) {
        return false;
      }
      if (this.input.next()) {
        this.remain--;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Object get(final int attributeIndex) {
      return this.input.get(attributeIndex);
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec attributeCodec) {
      return this.input.get(attributeIndex, attributeCodec);
    }

    @Override
    public void close() throws IOException {
      this.input.close();
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultLimitIterator(
        this.executeEngine,
        executeParameters,
        this.input.duplicate(executeParameters),
        this.getSchema(),
        this.limit
      );
    }
  }

  private static final class ResultGetIterator extends ResultIterator {

    private static final Result UNKNOWN = new Result();

    ResultGetIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final HBaseTableReference tableReference,
        final List<Reference<?>> tableColumnReferenceList,
        final IndexPrefixMatchResult primaryKeyMatchResult,
        final Schema resultSchema
    ) {
      super(executeEngine, executeParameters, resultSchema);
      this.tableReference = tableReference;
      this.tableColumnReferenceList = tableColumnReferenceList;
      this.table = null;
      this.primaryKeyMatchResult = primaryKeyMatchResult;
      this.result = UNKNOWN;
    }

    private final HBaseTableReference tableReference;

    private final List<Reference<?>> tableColumnReferenceList;

    private org.apache.hadoop.hbase.client.Table table;

    private final IndexPrefixMatchResult primaryKeyMatchResult;

    private Result result;

    @Override
    public boolean next() throws IOException {
      if (this.result == UNKNOWN) {
        if (this.table == null) {
          this.table = this.executeEngine.getConnection().getTable(this.tableReference.getIdentifier().getFullTableName());
        }
        final Result result = this.table.get(this.buildGet());
        this.result = result.getRow() == null ? null : result;
        return this.result != null;
      } else {
        if (this.result != null) {
          this.result = null;
        }
        return false;
      }
    }

    @Override
    public Object get(final int attributeIndex) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      final Reference<?> tableColumnReference = this.tableColumnReferenceList.get(attributeIndex);
      final TableDefinition.Column<?> tableColumn = this.tableReference.getColumn(tableColumnReference.getAttributeIndex());
      final Integer columnIndexInPrimaryKey = this.tableReference.getPrimaryKey().getColumnIndex(tableColumn);
      if (columnIndexInPrimaryKey != null) {
        final IndexPrefixMatchResult primaryKeyMatchResult = this.primaryKeyMatchResult;
        if (columnIndexInPrimaryKey < primaryKeyMatchResult.getPrefix().getDimension()) {
          return primaryKeyMatchResult.getPrefix().get(columnIndexInPrimaryKey);
        } else {
          return primaryKeyMatchResult.getScanInterval().getMinimum();
        }
      }
      final Integer columnIndexInTable = tableColumnReference.getAttributeIndex();
      final byte[] valueBytes = this.tableReference.getColumnReference(columnIndexInTable).getValue(this.result);
      if (valueBytes == null) {
        return null;
      } else {
        return this.tableReference.getColumnCodec(columnIndexInTable).decode(valueBytes);
      }
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      if (resultCodec == this.tableReference.getColumnCodec(attributeIndex)) {

      } else {

      }
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      if (this.table != null) {
        this.table.close();
      }
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultGetIterator(
          this.executeEngine,
          executeParameters,
          this.tableReference,
          this.tableColumnReferenceList,
          this.primaryKeyMatchResult,
          this.getSchema()
      );
    }

    private Get buildGet() {
      final PrimaryKeyValueIterator primaryKeyValueIterator = PrimaryKeyValueIterator.CACHE.get();
      try {
        primaryKeyValueIterator.setPrimaryKeyValueList(this.primaryKeyMatchResult, ValueRange.IntervalBoundary.MINIMUM);
        final byte[] primaryKey = this.tableReference.getPrimaryKeyCodec().encode(primaryKeyValueIterator);
        final Get get = new Get(primaryKey);
        for (Reference<?> tableColumnReference : this.tableColumnReferenceList) {
          final HBaseTableColumnReference<?> columnReference = this.tableReference.getColumnReference(tableColumnReference.getAttributeIndex());
          if (columnReference.getFamily() == null) {
            continue;
          }
          columnReference.setTo(get);
        }
        if (!get.hasFamilies()) {
          this.tableReference.setRowMark(get);
        }
        return get;
      } finally {
        primaryKeyValueIterator.clean();
      }
    }

  }

  private static final class ResultScanIterator extends ResultIterator {

    private static final Result UNKNOWN = new Result();

    ResultScanIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final HBaseTableReference tableReference,
        final List<Reference<?>> tableColumnReferenceList,
        final List<IndexPrefixMatchResult> primaryKeyMatchResultList,
        final Schema resultSchema
    ) {
      super(executeEngine, executeParameters, resultSchema);
      this.tableReference = tableReference;
      this.tableColumnReferenceList = tableColumnReferenceList;
      this.table = null;
      this.primaryKeyCodec = tableReference.getPrimaryKeyCodec();
      this.primaryKeyDecoder = null;
      this.primaryKeyMatchResultList = primaryKeyMatchResultList;
      this.primaryKeyMatchResultIndex = 0;
      this.resultScanner = null;
      this.result = UNKNOWN;
    }

    private final HBaseTableReference tableReference;

    private final List<Reference<?>> tableColumnReferenceList;

    private org.apache.hadoop.hbase.client.Table table;

    private final HBaseRowKeyCodec primaryKeyCodec;

    private HBaseRowKeyCodec.Decoder primaryKeyDecoder;

    private final List<IndexPrefixMatchResult> primaryKeyMatchResultList;

    private int primaryKeyMatchResultIndex;

    private ResultScanner resultScanner;

    private Result result;

    @Override
    public boolean next() throws IOException {
      if (this.result == UNKNOWN) {
        if (this.table == null) {
          this.table = this.executeEngine.getConnection().getTable(this.tableReference.getIdentifier().getFullTableName());
        }
      }
      if (this.resultScanner != null) {
        Result result = this.resultScanner.next();
        while (result != null && result.getRow() == null) {
          result = this.resultScanner.next();
        }
        if (result != null) {
          this.result = result;
          return true;
        }
      }
      final PrimaryKeyValueIterator primaryKeyValueIterator = PrimaryKeyValueIterator.CACHE.get();
      try {
        while (this.primaryKeyMatchResultIndex < this.primaryKeyMatchResultList.size()) {
          final IndexPrefixMatchResult primaryKeyMatchResult = this.primaryKeyMatchResultList.get(this.primaryKeyMatchResultIndex);
          this.primaryKeyMatchResultIndex++;
          final Get get = this.buildGet(primaryKeyMatchResult, primaryKeyValueIterator);
          if (get == null) {
            final org.apache.hadoop.hbase.client.Scan scan = this.buildScan(primaryKeyMatchResult, primaryKeyValueIterator);
            final ResultScanner resultScanner = this.table.getScanner(scan);
            this.result = resultScanner.next();
            if (this.result != null) {
              this.resultScanner = resultScanner;
              return true;
            }
          } else {
            if (this.primaryKeyMatchResultIndex < this.primaryKeyMatchResultList.size()) {
              Get nextGet = this.buildGet(this.primaryKeyMatchResultList.get(this.primaryKeyMatchResultIndex), primaryKeyValueIterator);
              if (nextGet != null) {
                final List<Get> getList = new ArrayList<>();
                getList.add(get);
                getList.add(nextGet);
                this.primaryKeyMatchResultIndex++;
                while (this.primaryKeyMatchResultIndex < this.primaryKeyMatchResultList.size()) {
                  nextGet = this.buildGet(this.primaryKeyMatchResultList.get(this.primaryKeyMatchResultIndex), primaryKeyValueIterator);
                  if (nextGet != null) {
                    getList.add(nextGet);
                    this.primaryKeyMatchResultIndex++;
                    if (getList.size() == 10) {// TODO 通过配置项配置
                      break;
                    }
                  } else {
                    break;
                  }
                }
                final ResultScanner resultScanner = new ResultsScanner(this.table.get(getList));
                Result result = resultScanner.next();
                while (result != null && result.getRow() == null) {
                  result = resultScanner.next();
                }
                if (result != null) {
                  this.resultScanner = resultScanner;
                  this.result = result;
                  return true;
                }
                continue;
              }
            }
            final Result result = this.table.get(get);
            if (result != null && result.getRow() != null) {
              this.resultScanner = null;
              this.result = result;
              return true;
            }
          }
        }
      } finally {
        primaryKeyValueIterator.clean();
      }
      this.resultScanner = null;
      this.result = null;
      return false;
    }

    @Override
    public Object get(final int attributeIndex) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      final Reference<?> tableColumnReference = this.tableColumnReferenceList.get(attributeIndex);
      final TableDefinition.Column<?> tableColumn = this.tableReference.getColumn(tableColumnReference.getAttributeIndex());
      final Integer columnIndexInPrimaryKey = this.tableReference.getPrimaryKey().getColumnIndex(tableColumn);
      if (columnIndexInPrimaryKey != null) {
        if (this.primaryKeyDecoder == null) {
          this.primaryKeyDecoder = this.primaryKeyCodec.decoder();
        }
        this.primaryKeyDecoder.setRowKey(this.result.getRow());
        for (int index = 0; index <= columnIndexInPrimaryKey; index++) {
          this.primaryKeyDecoder.next();
        }
        return this.primaryKeyDecoder.get();
      } else {
        final Integer columnIndexInTable = tableColumnReference.getAttributeIndex();
        final byte[] valueBytes = this.tableReference.getColumnReference(columnIndexInTable).getValue(this.result);
        if (valueBytes == null) {
          return null;
        } else {
          return this.tableReference.getColumnCodec(columnIndexInTable).decode(valueBytes);
        }
      }
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      if (resultCodec == this.tableReference.getColumnCodec(attributeIndex)) {

      } else {

      }
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      if (this.table != null) {
        this.table.close();
      }
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultScanIterator(
          this.executeEngine,
          executeParameters,
          this.tableReference,
          this.tableColumnReferenceList,
          this.primaryKeyMatchResultList,
          this.getSchema()
      );
    }

    @Nullable
    private Get buildGet(
        final IndexPrefixMatchResult primaryKeyMatchResult,
        final PrimaryKeyValueIterator primaryKeyValueIterator
    ) {
      if (isGet(primaryKeyMatchResult, this.tableReference.getPrimaryKey())) {
        // Full matched.
        final byte[] primaryKey = this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MINIMUM);
        final Get get = new Get(primaryKey);
        for (Reference<?> tableColumnReference : this.tableColumnReferenceList) {
          final HBaseTableColumnReference<?> columnReference = this.tableReference.getColumnReference(tableColumnReference.getAttributeIndex());
          if (columnReference.getFamily() == null) {
            continue;
          }
          columnReference.setTo(get);
        }
        if (!get.hasFamilies()) {
          this.tableReference.setRowMark(get);
        }
        return get;
      }
      return null;
    }

    @Nonnull
    private org.apache.hadoop.hbase.client.Scan buildScan(
        final IndexPrefixMatchResult primaryKeyMatchResult,
        final PrimaryKeyValueIterator primaryKeyValueIterator
    ) {
      final org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
      final ValueRange.Interval<?> scanInterval = primaryKeyMatchResult.getScanInterval();
      if (primaryKeyMatchResult.getPrefix().getDimension() == 0 && primaryKeyMatchResult.getScanInterval().isInfinite()) {
        if (scanInterval.getMinimum() != null) {
          scan.withStartRow(
              this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MINIMUM),
              scanInterval.isIncludeMinimum()
          );
        }
        if (scanInterval.getMaximum() != null) {
          scan.withStopRow(
              this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MAXIMUM),
              scanInterval.isIncludeMaximum()
          );
        }
      } else {
        scan.withStartRow(
            this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MINIMUM),
            scanInterval.isIncludeMinimum()
        );
        if (scanInterval.getMaximum() != null) {
          scan.withStopRow(
              this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MAXIMUM),
              scanInterval.isIncludeMaximum()
          );
        } else {
          scan.withStopRow(
              HBaseRowKeyHelper.next(this.buildPrimaryKey(primaryKeyMatchResult, primaryKeyValueIterator, ValueRange.IntervalBoundary.MAXIMUM)),
              false
          );
        }
      }
      for (Reference<?> tableColumnReference : this.tableColumnReferenceList) {
        final HBaseTableColumnReference<?> columnReference = this.tableReference.getColumnReference(tableColumnReference.getAttributeIndex());
        if (columnReference.getFamily() == null) {
          continue;
        }
        columnReference.setTo(scan);
      }
      if (!scan.hasFamilies()) {
        this.tableReference.setRowMark(scan);
      }
      return scan;
    }

    private byte[] buildPrimaryKey(
        final IndexPrefixMatchResult primaryKeyMatchResult,
        final PrimaryKeyValueIterator primaryKeyValueIterator,
        final ValueRange.IntervalBoundary intervalBoundary
    ) {
      primaryKeyValueIterator.setPrimaryKeyValueList(primaryKeyMatchResult, intervalBoundary);
      if (intervalBoundary.isInfinite(primaryKeyMatchResult.getScanInterval())) {
        return this.primaryKeyCodec.encode(primaryKeyValueIterator, primaryKeyMatchResult.getPrefix().getDimension());
      } else {
        return this.primaryKeyCodec.encode(primaryKeyValueIterator, primaryKeyMatchResult.getPrefix().getDimension() + 1);
      }
    }

  }

  private static final class ResultFullScanIterator extends ResultIterator {

    private static final Result UNKNOWN = new Result();

    ResultFullScanIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final HBaseTableReference tableReference,
        final List<Reference<?>> tableColumnReferenceList,
        final Schema resultSchema
    ) {
      super(executeEngine, executeParameters, resultSchema);
      this.tableReference = tableReference;
      this.tableColumnReferenceList = tableColumnReferenceList;
      this.filterCondition = null;
      this.table = null;
      this.primaryKeyDecoder = null;
      this.resultScanner = null;
      this.result = UNKNOWN;
    }

    ResultFullScanIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final HBaseTableReference tableReference,
        final List<Reference<?>> tableColumnReferenceList,
        final Schema resultSchema,
        final Evaluation<Boolean> filterCondition
    ) {
      super(executeEngine, executeParameters, resultSchema);
      this.tableReference = tableReference;
      this.tableColumnReferenceList = tableColumnReferenceList;
      this.filterCondition = filterCondition;
      this.table = null;
      this.primaryKeyDecoder = null;
      this.resultScanner = null;
      this.result = UNKNOWN;
    }

    private final HBaseTableReference tableReference;

    private final List<Reference<?>> tableColumnReferenceList;

    private final Evaluation<Boolean> filterCondition;

    private org.apache.hadoop.hbase.client.Table table;

    private HBaseRowKeyCodec.Decoder primaryKeyDecoder;

    private ResultScanner resultScanner;

    private Result result;

    @Override
    public boolean next() throws IOException {
      if (this.result == UNKNOWN) {
        if (this.table == null) {
          this.table = this.executeEngine.getConnection().getTable(this.tableReference.getIdentifier().getFullTableName());
        }
      }
      if (this.resultScanner != null) {
        this.result = this.resultScanner.next();
        if (this.result != null) {
          return true;
        }
      } else {
        final ResultScanner resultScanner = this.table.getScanner(this.buildScan());
        this.result = resultScanner.next();
        if (this.result != null) {
          this.resultScanner = resultScanner;
          return true;
        }
      }
      this.resultScanner = null;
      return false;
    }

    @Override
    public Object get(final int attributeIndex) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      final Reference<?> tableColumnReference = this.tableColumnReferenceList.get(attributeIndex);
      final TableDefinition.Column<?> tableColumn = this.tableReference.getColumn(tableColumnReference.getAttributeIndex());
      final Integer columnIndexInPrimaryKey = this.tableReference.getPrimaryKey().getColumnIndex(tableColumn);
      if (columnIndexInPrimaryKey != null) {
        if (this.primaryKeyDecoder == null) {
          this.primaryKeyDecoder = this.tableReference.getPrimaryKeyCodec().decoder();
        }
        this.primaryKeyDecoder.setRowKey(this.result.getRow());
        for (int index = 0; index <= columnIndexInPrimaryKey; index++) {
          this.primaryKeyDecoder.next();
        }
        return this.primaryKeyDecoder.get();
      } else {
        final Integer columnIndexInTable = tableColumnReference.getAttributeIndex();
        final byte[] valueBytes = this.tableReference.getColumnReference(columnIndexInTable).getValue(this.result);
        if (valueBytes == null) {
          return null;
        } else {
          return this.tableReference.getColumnCodec(columnIndexInTable).decode(valueBytes);
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
      if (this.result == UNKNOWN) {
        throw new IllegalStateException();
      }
      if (this.result == null) {
        throw new NoSuchElementException();
      }
      final Reference<?> tableColumnReference = this.tableColumnReferenceList.get(attributeIndex);
      final TableDefinition.Column<?> tableColumn = this.tableReference.getColumn(tableColumnReference.getAttributeIndex());
      final Integer columnIndexInPrimaryKey = this.tableReference.getPrimaryKey().getColumnIndex(tableColumn);
      final ValueBytes columnValueBytes;
      if (columnIndexInPrimaryKey != null) {
        if (this.primaryKeyDecoder == null) {
          this.primaryKeyDecoder = this.tableReference.getPrimaryKeyCodec().decoder();
        }
        this.primaryKeyDecoder.setRowKey(this.result.getRow());
        for (int index = 0; index <= columnIndexInPrimaryKey; index++) {
          this.primaryKeyDecoder.next();
        }
        columnValueBytes = this.primaryKeyDecoder.getAsBytes();
        if (columnValueBytes == null) {
          return null;
        }
      } else {
        final Integer columnIndexInTable = tableColumnReference.getAttributeIndex();
        final byte[] valueBytes = this.tableReference.getColumnReference(columnIndexInTable).getValue(this.result);
        if (valueBytes == null) {
          return null;
        }
        columnValueBytes = ValueBytes.of(valueBytes);// TODO 不要每次都new
      }
      final ValueCodec columnCodec = this.tableReference.getColumnCodec(attributeIndex);
      if (resultCodec == columnCodec) {
        return columnValueBytes;
      } else {
        return ValueBytes.of(resultCodec.encode(columnValueBytes.decode(columnCodec)));// TODO 不要每次都new
      }
    }

    @Override
    public void close() throws IOException {
      if (this.table != null) {
        this.table.close();
      }
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultFullScanIterator(
          this.executeEngine,
          executeParameters,
          this.tableReference,
          this.tableColumnReferenceList,
          this.getSchema()
      );
    }

    @Nonnull
    private org.apache.hadoop.hbase.client.Scan buildScan() {
      final org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
      for (Reference<?> tableColumnReference : this.tableColumnReferenceList) {
        final HBaseTableColumnReference<?> columnReference = this.tableReference.getColumnReference(tableColumnReference.getAttributeIndex());
        if (columnReference.getFamily() == null) {
          continue;
        }
        columnReference.setTo(scan);
      }
      if (this.filterCondition != null) {
        scan.setFilter(new ConditionFilter(this.tableReference, this.filterCondition));
      }
      if (!scan.hasFamilies()) {
        this.tableReference.setRowMark(scan);
      }
      return scan;
    }

  }

  private static final class ResultFilterIterator extends ResultIterator {

    ResultFilterIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final ResultIterator input,
        final Evaluation<Boolean> condition
    ) {
      super(executeEngine, executeParameters, input.getSchema());
      this.input = input;
      this.condition = condition;
      this.runtimeContext = new RuntimeContext(executeEngine, executeParameters, input);
    }

    private final ResultIterator input;

    private final Evaluation<Boolean> condition;

    private final Evaluation.RuntimeContext runtimeContext;

    @Override
    public boolean next() throws IOException {
      while (this.input.next()) {
        try {
          if (this.condition.evaluate(this.runtimeContext)) {
            return true;
          }
        } catch (EvaluateException exception) {
          // TODO
          throw new UnsupportedOperationException(exception);
        }
      }
      return false;
    }

    @Override
    public Object get(final int attributeIndex) {
      return this.input.get(attributeIndex);
    }

    @Override
    public <TValue> TValue get(final int attributeIndex, final Class<TValue> resultClass) {
      return this.input.get(attributeIndex, resultClass);
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
      return this.input.get(attributeIndex, resultCodec);
    }

    @Override
    public void close() throws IOException {
      this.input.close();
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultFilterIterator(
          this.executeEngine,
          executeParameters,
          this.input.duplicate(executeParameters),
          this.condition
      );
    }

  }

  private static final class ResultProjectIterator extends ResultIterator {

    ResultProjectIterator(
        final HBaseExecuteEngine executeEngine,
        final ExecuteParameters executeParameters,
        final ResultIterator input,
        final Schema schema,
        final List<? extends Evaluation<?>> projectList
    ) {
      super(executeEngine, executeParameters, schema);
      this.input = input;
      this.projectList = projectList;
      this.runtimeContext = new RuntimeContext(executeEngine, executeParameters, input);
    }

    private final ResultIterator input;

    private final List<? extends Evaluation<?>> projectList;

    private final Evaluation.RuntimeContext runtimeContext;

    @Override
    public boolean next() throws IOException {
      return this.input.next();
    }

    @Override
    public Object get(final int attributeIndex) {
      try {
        return this.projectList.get(attributeIndex).evaluate(this.runtimeContext);
      } catch (EvaluateException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }

    @Override
    public ValueBytes get(final int attributeIndex, final ValueCodec resultCodec) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      this.input.close();
    }

    @Override
    ResultIterator duplicate(final ExecuteParameters executeParameters) {
      return new ResultProjectIterator(
          this.executeEngine,
          executeParameters,
          this.input.duplicate(executeParameters),
          this.getSchema(),
          this.projectList
      );
    }

  }

  private static final class ExpressionReconstructor extends com.huawei.cloudtable.leo.ExpressionReconstructor<ExpressionReconstructor.Context> {

    static final ExpressionReconstructor INSTANCE = new ExpressionReconstructor();

    @Override
    public Evaluation visit(final Reference<?> reference, final Context context) {
      final Reference<?> tableColumnReference = context.getTableColumnReference(reference.getAttributeIndex());
      return tableColumnReference.getAttributeIndex() == reference.getAttributeIndex() ? reference : tableColumnReference;
    }

    static final class Context extends com.huawei.cloudtable.leo.ExpressionReconstructor.Context {

      private static final ThreadLocal<Context> CACHE = new ThreadLocal<Context>() {
        @Override
        protected Context initialValue() {
          return new Context();
        }
      };

      static Context get() {
        return CACHE.get();
      }

      private Context() {
        // to do nothing.
      }

      private FunctionManager functionManager;

      private List<Reference<?>> tableColumnReferenceList;

      @Override
      public FunctionManager getFunctionManager() {
        return this.functionManager;
      }

      Reference<?> getTableColumnReference(final int index) {
        return this.tableColumnReferenceList.get(index);
      }

      void reset(final FunctionManager functionManager, final List<Reference<?>> tableColumnReferenceList) {
        this.functionManager = functionManager;
        this.tableColumnReferenceList = tableColumnReferenceList;
      }

    }

  }

  public static final class PrimaryKeyValueIterator extends HBaseRowKeyValueIterator {

   public static final ThreadLocal<PrimaryKeyValueIterator> CACHE = new ThreadLocal<PrimaryKeyValueIterator>() {
      @Override
      protected PrimaryKeyValueIterator initialValue() {
        return new PrimaryKeyValueIterator();
      }
    };

    private PrimaryKeyValueIterator() {
      // to do nothing.
    }

    private IndexPrefixMatchResult primaryKeyMatchResult;

    private ValueRange.IntervalBoundary primaryKeyMatchResultBoundary;

    private int currentValueIndex = -1;

    private int theLastValueIndex = -1;

    @SuppressWarnings("unchecked")
    public void setPrimaryKeyValueList(
        final IndexPrefixMatchResult primaryKeyMatchResult,
        final ValueRange.IntervalBoundary primaryKeyMatchResultBoundary
    ) {
      final boolean theLastSegmentIsInfinite = primaryKeyMatchResultBoundary.isInfinite(primaryKeyMatchResult.getScanInterval());
      this.primaryKeyMatchResult = primaryKeyMatchResult;
      this.primaryKeyMatchResultBoundary = primaryKeyMatchResultBoundary;
      this.currentValueIndex = -1;
      this.theLastValueIndex = theLastSegmentIsInfinite ? primaryKeyMatchResult.getPrefix().getDimension() : primaryKeyMatchResult.getPrefix().getDimension() + 1;
    }

    @Override
    public boolean hasNext() {
      return this.currentValueIndex < this.theLastValueIndex;
    }

    @Override
    public void next() {
      if (this.hasNext()) {
        this.currentValueIndex++;
      } else {
        throw new RuntimeException("No more data.");
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object get() {
      if (this.currentValueIndex < this.primaryKeyMatchResult.getPrefix().getDimension()) {
        return this.primaryKeyMatchResult.getPrefix().get(this.currentValueIndex);
      } else {
        return this.primaryKeyMatchResultBoundary.get(this.primaryKeyMatchResult.getScanInterval());
      }
    }

    @Override
    public ValueBytes getAsBytes() {
      throw new UnsupportedOperationException();
    }

    void clean() {
      this.primaryKeyMatchResult = null;
      this.primaryKeyMatchResultBoundary = null;
      this.currentValueIndex = -1;
      this.theLastValueIndex = -1;
    }

  }

}
