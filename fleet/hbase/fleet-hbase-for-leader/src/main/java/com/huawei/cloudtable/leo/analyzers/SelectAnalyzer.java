package com.huawei.cloudtable.leo.analyzers;

import com.huawei.cloudtable.leo.*;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyseContext;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyzer;
import com.huawei.cloudtable.leo.analyzer.SemanticException;
import com.huawei.cloudtable.leo.analyzer.SemanticException.Reason;
import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.expressions.FirstExpression;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.expression.*;
import com.huawei.cloudtable.leo.language.statement.*;
import com.huawei.cloudtable.leo.language.statements.SQLSelectStatement;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.metadata.TableReference;
import com.huawei.cloudtable.leo.relations.LogicalAggregate;
import com.huawei.cloudtable.leo.relations.LogicalProject;
import com.huawei.cloudtable.leo.relations.LogicalScan;
import com.huawei.cloudtable.leo.statements.SelectStatement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public final class SelectAnalyzer extends SemanticAnalyzer<SQLSelectStatement, SelectStatement> {

  public SelectAnalyzer() {
    super(SQLSelectStatement.class, SelectStatement.class);
  }

  @Override
  public SelectStatement analyse(final SemanticAnalyseContext context, final SQLSelectStatement sourceStatement) throws SemanticException, ExecuteException {
    final LogicalQuery logicalQuery = toLogicalQuery(context, sourceStatement);
    final LogicalTable logicalTable = logicalQuery.getLogicalTable();
    final LogicalSelect logicalSelect = logicalQuery.getLogicalSelect();
    final int relationAttributeCount;
    final Relation relation;
    if (logicalTable.getAttributeCount() > logicalSelect.getProjectCount()) {
      relationAttributeCount = logicalSelect.getProjectCount();
      relation = new LogicalProject(logicalTable.toRelation(context), logicalTable.getAttributeReferences().subList(0, relationAttributeCount));// TODO 可以考虑直接从LogicalSelect取，不必要取全部
    } else {
      relationAttributeCount = logicalTable.getAttributeCount();
      relation = logicalTable.toRelation(context);
    }
    final List<String> relationAttributeTitles = new ArrayList<>(relationAttributeCount);
    for (int index = 0; index < relationAttributeCount; index++) {
      relationAttributeTitles.add(logicalTable.getAttributeTitle(index));
    }
    return new SelectStatement(relation, relationAttributeTitles, analyseHints(sourceStatement.getQuery()));
  }

  private static Set<String> analyseHints(final SQLQuery sourceQuery) {
    if (sourceQuery instanceof SQLQuery.Select) {
      final SQLHints sourceHints = ((SQLQuery.Select) sourceQuery).getHints();
      if (sourceHints != null && !sourceHints.get().isEmpty()) {
        final Set<String> hints = new HashSet<>(sourceHints.get().size());
        for (SQLHint sourceHint : sourceHints.get()) {
          hints.add(sourceHint.getName().toString());
        }
        return hints;
      }
    }
    return Collections.emptySet();
  }

  private static LogicalQuery toLogicalQuery(final SemanticAnalyseContext context, final SQLSelectStatement sourceStatement) throws SemanticException, ExecuteException {
    final LogicalQuery logicalQuery = toLogicalQuery(context, sourceStatement.getQuery());
    LogicalTable logicalTable = logicalQuery.logicalTable;
    if (sourceStatement.getOffset() != null) {
      logicalTable = new LogicalOffset(sourceStatement.getOffset(), logicalTable);
    }
    if (sourceStatement.getLimit() != null) {
      logicalTable = new LogicalLimit(sourceStatement.getLimit(), logicalTable);
    }
    return new LogicalQuery(logicalTable, logicalQuery.getLogicalSelect());
  }

  private static LogicalQuery toLogicalQuery(final SemanticAnalyseContext context, final SQLQuery sourceQuery) throws SemanticException, ExecuteException {
    if (sourceQuery instanceof SQLQuery.Select) {
      return toLogicalQuery(context, (SQLQuery.Select) sourceQuery);
    }
    throw new UnsupportedOperationException();
  }

  private static LogicalQuery toLogicalQuery(
      final SemanticAnalyseContext context,
      final SQLQuery.Select sourceQuery
  ) throws SemanticException, ExecuteException {
    LogicalTable logicalTable = LogicalFrom.toLogicalTable(context, sourceQuery.getFrom());
    if (sourceQuery.getWhere() != null) {
      logicalTable = new LogicalWhere(context, sourceQuery.getWhere(), logicalTable);
    }
    final LogicalSelect logicalSelect = new LogicalSelect(context, sourceQuery.getProjects(), sourceQuery.getGroupBy(), logicalTable);
    if (sourceQuery.getHaving() != null) {
      logicalTable = new LogicalHaving(context, sourceQuery.getHaving(), logicalSelect);
    } else {
      logicalTable = logicalSelect;
    }
    return new LogicalQuery(logicalTable, logicalSelect);
  }

  private static final class LogicalQuery {

    LogicalQuery(final LogicalTable logicalTable, final LogicalSelect logicalSelect) {
      this.logicalTable = logicalTable;
      this.logicalSelect = logicalSelect;
    }

    private final LogicalTable logicalTable;

    private final LogicalSelect logicalSelect;

    LogicalTable getLogicalTable() {
      return this.logicalTable;
    }

    LogicalSelect getLogicalSelect() {
      return this.logicalSelect;
    }

  }

  private static abstract class LogicalTable extends ReferenceResolver {

    LogicalTable() {
      // to do nothing.
    }

    abstract int getAttributeCount();

    @Nonnull
    abstract String getAttributeTitle(int attributeIndex);

    // TODO 该方法主要用于检测Project的别名是否可能跟列名冲突的场景
    @Nonnull
    public abstract Reference<?> getAttributeReference(Identifier columnName) throws SemanticException;

    @Override
    @Nonnull
    public abstract Reference<?> getAttributeReference(SQLReference.Column sourceReference) throws SemanticException;

    @Nonnull
    public abstract List<Reference<?>> getAttributeReferences();

    @Override
    @Nonnull
    public abstract List<Reference<?>> getAttributeReferences(SQLReference.ColumnAll sourceReference) throws SemanticException;

    @Nonnull
    @Override
    public abstract Reference<?> getAggregationReference(SQLExpression sourceExpression, SemanticAnalyseContext context) throws SemanticException;

    @Nullable
    Reference<?> tryGetAttributeReference(final Identifier columnName) throws SemanticException {
      try {
        return this.getAttributeReference(columnName);
      } catch (SemanticException exception) {
        if (exception.getReason() == Reason.COLUMN_NOT_FOUND) {
          return null;
        } else {
          throw exception;
        }
      }
    }

    @Nullable
    Reference<?> tryGetAttributeReference(SQLReference.Column sourceReference) throws SemanticException {
      try {
        return this.getAttributeReference(sourceReference);
      } catch (SemanticException exception) {
        if (exception.getReason() == Reason.COLUMN_NOT_FOUND) {
          return null;
        } else {
          throw exception;
        }
      }
    }

    @Nullable
    List<Reference<?>> tryGetAttributeReference(SQLReference.ColumnAll sourceReference) throws SemanticException {
      try {
        return this.getAttributeReferences(sourceReference);
      } catch (SemanticException exception) {
        if (exception.getReason() == Reason.COLUMN_NOT_FOUND) {
          return null;
        } else {
          throw exception;
        }
      }
    }

    abstract Relation toRelation(SemanticAnalyseContext context) throws SemanticException, ExecuteException;

    protected static void checkRelationSchemaCompatibility(final Relation base, final Relation relation) throws SemanticException {
      final Table.Schema baseSchema = base.getResultSchema();
      final Table.Schema relationSchema = relation.getResultSchema();
      final int attributeCount = baseSchema.getAttributeCount();
      if (relationSchema.getAttributeCount() != attributeCount) {
        throw new SemanticException(Reason.COLUMN_COUNT_DIFFERENT, null);
      }
      for (int index = 0; index < attributeCount; index++) {
        final Table.Schema.Attribute inputLeftAttribute = baseSchema.getAttribute(index);
        final Table.Schema.Attribute inputRightAttribute = relationSchema.getAttribute(index);
        if (inputLeftAttribute.getValueClass() != inputRightAttribute.getValueClass()) {// TODO 隐式转换后，是否兼容，而不是简单的相等
          throw new SemanticException(Reason.COLUMN_INCOMPATIBILITY, null);
        }
        if (!inputLeftAttribute.isNullable() && inputRightAttribute.isNullable()) {
          throw new SemanticException(Reason.COLUMN_INCOMPATIBILITY, null);
        }
      }
    }

  }

  private static abstract class LogicalFilter extends LogicalTable {

    @SuppressWarnings("unchecked")
    protected static Evaluation<Boolean> toCondition(
        final SemanticAnalyseContext context,
        final SQLExpression sourceCondition,
        final LogicalTable input
    ) throws SemanticException {
      final Expression<?> expression = buildExpression(context, sourceCondition, input);
      if (!(expression instanceof Evaluation)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (expression.getResultClass() != Boolean.class) {
        // TODO 抛异常？MySQL并未抛异常，而是给出告警
        throw new UnsupportedOperationException();
      }
      return (Evaluation<Boolean>) expression;
    }

    LogicalFilter(
        final SemanticAnalyseContext context,
        final SQLExpression sourceCondition,
        final LogicalTable input
    ) throws SemanticException {
      this.input = input;
      this.condition = toCondition(context, sourceCondition, input);
    }

    private final LogicalTable input;

    private final Evaluation<Boolean> condition;

    @Override
    int getAttributeCount() {
      return this.input.getAttributeCount();
    }

    @Nonnull
    @Override
    String getAttributeTitle(final int attributeIndex) {
      return this.input.getAttributeTitle(attributeIndex);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
      return this.input.getAttributeReference(columnName);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
      return this.input.getAttributeReference(sourceReference);
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences() {
      return this.input.getAttributeReferences();
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
      return this.input.getAttributeReferences(sourceReference);
    }

    @Nonnull
    @Override
    public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
      return this.input.getAggregationReference(sourceExpression, context);
    }

    @Override
    Relation toRelation(final SemanticAnalyseContext context) throws SemanticException, ExecuteException {
      return new com.huawei.cloudtable.leo.relations.LogicalFilter(this.input.toRelation(context), this.condition);
    }

  }

  private static abstract class LogicalFrom extends LogicalTable {

    static LogicalTable toLogicalTable(
        final SemanticAnalyseContext context,
        final SQLFrom sourceFrom
    ) throws SemanticException, ExecuteException {
      final SQLTableReference sourceTableReference = sourceFrom.getTableReference();
      if (sourceTableReference instanceof SQLTableReference.Named) {
        return new LogicalFromNamed(context, (SQLTableReference.Named) sourceTableReference);
      }
      throw new UnsupportedOperationException();
    }

    protected static LogicalTable toLogicalTable(
        final SemanticAnalyseContext context,
        final SQLTableReference sourceTableReference,
        final Set<Identifier> aliasSet
    ) throws SemanticException, ExecuteException {
      if (sourceTableReference instanceof SQLTableReference.Named) {
        final LogicalFromNamed logicalTable = new LogicalFromNamed(context, (SQLTableReference.Named) sourceTableReference);
        if (aliasSet.contains(logicalTable.getName())) {
          throw new SemanticException(Reason.ALIAS_NOT_UNIQUE, sourceTableReference);
        }
        return logicalTable;
      }
      throw new UnsupportedOperationException();
    }

    LogicalFrom() {
      // to do nothing.
    }

  }

  private static final class LogicalFromNamed extends LogicalFrom {

    LogicalFromNamed(final SemanticAnalyseContext context, final SQLTableReference.Named sourceTableReference)
        throws SemanticException {
      final TableReference targetTableReference;
      try {
        if (sourceTableReference.getSchemaName() == null) {
          targetTableReference = context.getExecuteEngine().getTableReference(
              context.getExecuteContext(),
              sourceTableReference.getTableName().getValue()
          );
        } else {
          targetTableReference = context.getExecuteEngine().getTableReference(
              context.getExecuteContext(),
              sourceTableReference.getSchemaName().getValue(),
              sourceTableReference.getTableName().getValue()
          );
        }
      } catch (ExecuteException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
      if (targetTableReference == null) {
        throw new SemanticException(Reason.TABLE_NOT_FOUND, sourceTableReference);
      }
      final Identifier name;
      if (sourceTableReference.getAlias() == null) {
        name = targetTableReference.getName();
      } else {
        name = sourceTableReference.getAlias().getValue();
      }
      this.name = name;
      this.tableReference = targetTableReference;
    }

    private final Identifier name;

    private final TableReference tableReference;

    private final List<Reference<?>> attributeReferences = new ArrayList<>();

    private final Map<Identifier, Reference<?>> attributeReferenceMapByName = new HashMap<>();

    private final List<Reference<?>> tableColumnReferenceList = new ArrayList<>();

    private List<Reference<?>> attributeReferencesWithOriginalOrder;

    @Nonnull
    Identifier getName() {
      return this.name;
    }

    @Override
    int getAttributeCount() {
      return this.tableReference.getColumnCount();
    }

    @Nonnull
    @Override
    String getAttributeTitle(final int attributeIndex) {
      final Reference<?> tableColumnReference = this.tableColumnReferenceList.get(attributeIndex);
      return this.tableReference.getColumn(tableColumnReference.getAttributeIndex()).getName().toString();
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
      Reference<?> attributeReference = this.attributeReferenceMapByName.get(columnName);
      if (attributeReference == null) {
        final TableDefinition.Column<?> targetTableColumn = this.tableReference.getColumn(columnName);
        if (targetTableColumn != null) {
          attributeReference = this.putAttributeReference(targetTableColumn);
        }
      }
      if (attributeReference == null) {
        throw new SemanticException(Reason.COLUMN_NOT_FOUND, null, columnName.toString());
      } else {
        return attributeReference;
      }
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
      if (sourceReference.getTableName() != null
          && !sourceReference.getTableName().getValue().equals(this.name)) {
        throw new SemanticException(Reason.COLUMN_NOT_FOUND, sourceReference);
      }
      try {
        return this.getAttributeReference(sourceReference.getColumnName().getValue());
      } catch (SemanticException exception) {
        if (exception.getReason() == Reason.COLUMN_NOT_FOUND) {
          throw new SemanticException(Reason.COLUMN_NOT_FOUND, sourceReference);
        } else {
          throw exception;
        }
      }
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences() {
      if (this.attributeReferences.size() != this.tableReference.getColumnCount()) {
        for (int index = 0; index < this.tableReference.getColumnCount(); index++) {
          final TableDefinition.Column<?> targetTableColumn = this.tableReference.getColumn(index);
          if (this.attributeReferenceMapByName.get(targetTableColumn.getName()) == null) {
            this.putAttributeReference(targetTableColumn);
          }
        }
      }
      return Collections.unmodifiableList(this.attributeReferences);
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
      if (sourceReference.getTableName() != null
          && !sourceReference.getTableName().getValue().equals(this.name)) {
        throw new SemanticException(Reason.COLUMN_NOT_FOUND, sourceReference);
      }
      if (this.attributeReferencesWithOriginalOrder == null) {
        final List<Reference<?>> attributeReferences = this.getAttributeReferences();
        // 将所有attributeReference，按原始顺序排序后，返回，原始顺序为用户建表时指定的列顺序
        final Reference<?>[] attributeReferencesWithOriginalOrder = new Reference<?>[attributeReferences.size()];
        for (int index = 0; index < attributeReferences.size(); index++) {
          final Reference<?> attributeReference = attributeReferences.get(index);
          final int originalOrder = this.tableColumnReferenceList.get(attributeReference.getAttributeIndex()).getAttributeIndex();
          attributeReferencesWithOriginalOrder[originalOrder] = attributeReference;
        }
        this.attributeReferencesWithOriginalOrder = Collections.unmodifiableList(Arrays.asList(attributeReferencesWithOriginalOrder));
      }
      return this.attributeReferencesWithOriginalOrder;
    }

    @Nonnull
    @Override
    public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
      throw new SemanticException(SemanticException.Reason.ILLEGAL_USE_OF_AGGREGATION, null);
    }

    private Reference<?> putAttributeReference(final TableDefinition.Column<?> targetTableColumn) {
      final Reference<?> attributeReference = ReferenceFactory.of(
          this.attributeReferenceMapByName.size(),
          targetTableColumn.getValueClass(),
          targetTableColumn.isNullable()
      );
      this.attributeReferences.add(attributeReference);
      this.attributeReferenceMapByName.put(targetTableColumn.getName(), attributeReference);
      this.tableColumnReferenceList.add(ReferenceFactory.of(
          this.tableReference.getColumnIndex(targetTableColumn),
          targetTableColumn.getValueClass(),
          targetTableColumn.isNullable()
      ));
      return attributeReference;
    }

    @Override
    public Relation toRelation(final SemanticAnalyseContext context) {
      return new LogicalScan(this.tableReference, this.tableColumnReferenceList);
    }

  }

  private static final class LogicalWhere extends LogicalFilter {

    LogicalWhere(
        final SemanticAnalyseContext context,
        final SQLWhere sourceWhere,
        final LogicalTable input
    ) throws SemanticException {
      super(context, sourceWhere.getCondition(), input);
    }

  }

  private static final class LogicalSelect extends LogicalTable {

    LogicalSelect(
        final SemanticAnalyseContext context,
        final SyntaxTree.NodeList<SQLProject, ?> sourceProjectList,
        final SQLGroupBy sourceGroupBy,
        final LogicalTable input
    ) throws SemanticException {
      final AggregateLevel aggregateLevel = new AggregateLevel(context, sourceGroupBy, input);
      final List<Evaluation<?>> attributeList = new ArrayList<>(sourceProjectList.size());
      buildAttributeList(context, attributeList, sourceProjectList, aggregateLevel);
      int attributeIndex = -1;
      Map<Integer, String> attributeTitleMapByIndex = null;
      Map<Identifier, Integer> attributeIndexMapByAlias = null;
      boolean haveColumnAllInProjectList = false;
      final Map<Integer, SQLProject> projectMapByAttributeIndex = new HashMap<>(sourceProjectList.size());
      for (int index = 0; index < sourceProjectList.size(); index++) {
        final SQLProject sourceProject = sourceProjectList.get(index);
        final SQLExpression sourceProjectExpression = sourceProject.getExpression();
        if (sourceProjectExpression instanceof SQLReference.ColumnAll) {
          final List<Reference<?>> inputAttributeReferences = input.getAttributeReferences((SQLReference.ColumnAll) sourceProjectExpression);
          final int attributeCount = inputAttributeReferences.size();
          haveColumnAllInProjectList = true;
          if (attributeTitleMapByIndex == null) {
            attributeTitleMapByIndex = new HashMap<>();
          }
          for (int inputAttributeIndex = 0; inputAttributeIndex < attributeCount; inputAttributeIndex++) {
            attributeIndex++;
            attributeTitleMapByIndex.put(attributeIndex, input.getAttributeTitle(inputAttributeReferences.get(inputAttributeIndex).getAttributeIndex()));
          }
        } else {
          attributeIndex++;
          projectMapByAttributeIndex.put(attributeIndex, sourceProject);
          if (sourceProject.getAlias() != null) {
            final Identifier attributeAlias = sourceProject.getAlias().getValue();
            if (attributeIndexMapByAlias == null) {
              attributeIndexMapByAlias = new HashMap<>();
              attributeIndexMapByAlias.put(attributeAlias, attributeIndex);
            } else {
              if (attributeIndexMapByAlias.containsKey(attributeAlias)) {
                // 别名重复了, 会产生歧义。（MySQL在部分情况下是允许的）
                attributeIndexMapByAlias.put(attributeAlias, -1);
              } else {
                attributeIndexMapByAlias.put(attributeAlias, attributeIndex);
              }
            }
            attributeIndexMapByAlias.put(attributeAlias, attributeIndex);
          }
        }
      }

      this.queryString = context.getStatementString();
      this.haveColumnAllInProjectList = haveColumnAllInProjectList;
      this.projectList = sourceProjectList;
      this.projectCount = attributeList.size();
      this.projectMapByAttributeIndex = Collections.unmodifiableMap(projectMapByAttributeIndex);
      this.input = input;
      this.aggregateLevel = aggregateLevel;
      this.attributeList = attributeList;
      this.attributeIndexMapByAlias = attributeIndexMapByAlias == null ? null : Collections.unmodifiableMap(attributeIndexMapByAlias);
      this.attributeTitleMapByIndex = attributeTitleMapByIndex;
      this.attributeReferences = null;
      this.attributeReferenceMapByIndex = null;
      this.attributeReferenceMapByInputAttributeIndex = null;
    }

    private final String queryString;

    private final boolean haveColumnAllInProjectList;

    private final SyntaxTree.NodeList<SQLProject, ?> projectList;

    private final int projectCount;

    private final Map<Integer, SQLProject> projectMapByAttributeIndex;

    private LogicalTable input;

    private AggregateLevel aggregateLevel;

    private final List<Evaluation<?>> attributeList;

    private final Map<Identifier, Integer> attributeIndexMapByAlias;

    private Map<Integer, String> attributeTitleMapByIndex;

    private List<Reference<?>> attributeReferences;

    private Map<Integer, Reference<?>> attributeReferenceMapByIndex;

    private Map<Integer, Reference<?>> attributeReferenceMapByInputAttributeIndex;

    private Map<Identifier, List<Reference<?>>> attributeReferencesMapByInputName;

    /**
     *  ProjectCount只会小于或等于AttributeCount
     */
    int getProjectCount() {
      return this.projectCount;
    }

    @Override
    int getAttributeCount() {
      return this.attributeList.size();
    }

    @Nonnull
    @Override
    String getAttributeTitle(final int attributeIndex) {
      if (this.attributeTitleMapByIndex == null) {
        this.attributeTitleMapByIndex = new HashMap<>();
      } else {
        final String attributeTitle = this.attributeTitleMapByIndex.get(attributeIndex);
        if (attributeTitle != null) {
          return attributeTitle;
        }
      }
      final String attributeTitle;
      final SQLProject sourceProject = this.projectMapByAttributeIndex.get(attributeIndex);
      if (sourceProject == null) {
        throw new RuntimeException(Integer.toString(attributeIndex));
      }
      if (sourceProject.getAlias() != null) {
        attributeTitle = sourceProject.getAlias().getValue().toString();
      } else {
        attributeTitle = sourceProject.getExpression().getPosition().getString(this.queryString);
      }
      this.attributeTitleMapByIndex.put(attributeIndex, attributeTitle);
      return attributeTitle;
    }

    @Nonnull
    private Reference<?> getAttributeReference(final int attributeIndex) {
      Reference<?> attributeReference;
      if (this.attributeReferenceMapByIndex == null) {
        this.attributeReferenceMapByIndex = new HashMap<>();
        attributeReference = null;
      } else {
        attributeReference = this.attributeReferenceMapByIndex.get(attributeIndex);
      }
      if (attributeReference == null) {
        final Expression<?> attributeExpression = this.attributeList.get(attributeIndex);
        attributeReference = ReferenceFactory.of(attributeIndex, attributeExpression.getResultClass(), attributeExpression.isNullable());
        this.attributeReferenceMapByIndex.put(attributeIndex, attributeReference);
      }
      return attributeReference;
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
      return this.getAttributeReference(columnName, true);
    }

    public Reference<?> getAttributeReference(final Identifier columnName, final boolean drill) throws SemanticException {
      // 别名最优先，其次是输入表的列名
      if (this.attributeIndexMapByAlias != null) {
        final Integer attributeIndex = this.attributeIndexMapByAlias.get(columnName);
        if (attributeIndex != null) {
          if (attributeIndex < 0) {
            // 别名冲突的场景
            throw new SemanticException(Reason.ALIAS_NOT_UNIQUE, null, columnName.toString());
          }
          return this.getAttributeReference(attributeIndex);
        }
      }
      if (drill || this.haveColumnAllInProjectList) {
        return this.putAttributeReference(this.input.getAttributeReference(columnName));
      } else {
        throw new SemanticException(Reason.COLUMN_NOT_FOUND, null, columnName.toString());
      }
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
      if (sourceReference.getTableName() == null) {
        return this.getAttributeReference(sourceReference.getColumnName().getValue());
      } else {
        return this.putAttributeReference(this.input.getAttributeReference(sourceReference));
      }
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences() {
      if (this.attributeReferences == null) {
        final List<Reference<?>> attributeReferenceList = new ArrayList<>(this.getProjectCount());
        for (int index = 0; index < this.getProjectCount(); index++) {
          attributeReferenceList.add(this.getAttributeReference(index));
        }
        this.attributeReferences = attributeReferenceList;
      }
      return this.attributeReferences;
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
      if (sourceReference.getTableName() != null) {
        final Identifier inputName = sourceReference.getTableName().getValue();
        List<Reference<?>> attributeReferences;
        if (this.attributeReferencesMapByInputName == null) {
          this.attributeReferencesMapByInputName = new HashMap<>(1);
          attributeReferences = null;
        } else {
          attributeReferences = this.attributeReferencesMapByInputName.get(inputName);
        }
        if (attributeReferences == null) {
          final List<Reference<?>> inputAttributeReferences = this.input.getAttributeReferences(sourceReference);
          attributeReferences = new ArrayList<>(inputAttributeReferences.size());
          for (int index = 0; index < inputAttributeReferences.size(); index++) {
            attributeReferences.add(this.putAttributeReference(inputAttributeReferences.get(index)));
          }
          this.attributeReferencesMapByInputName.put(inputName, attributeReferences);
        }
        // 已按原始顺序排序，直接输出
        return attributeReferences;
      } else {
        // 已按原始顺序排序，直接输出
        return this.getAttributeReferences();
      }
    }

    private Reference<?> getAttributeReference(final Reference<?> inputAttributeReference) {
      if (this.attributeReferenceMapByInputAttributeIndex == null) {
        final Map<Integer, Reference<?>> attributeReferenceMapByInputAttributeIndex = new HashMap<>();
        for (int index = 0; index < this.attributeList.size(); index++) {
          final Expression<?> attributeExpression = this.attributeList.get(index);
          if (attributeExpression instanceof Reference) {
            attributeReferenceMapByInputAttributeIndex.put(
                ((Reference<?>) attributeExpression).getAttributeIndex(),
                this.getAttributeReference(index)
            );
          }
        }
        this.attributeReferenceMapByInputAttributeIndex = attributeReferenceMapByInputAttributeIndex;
      }
      return this.attributeReferenceMapByInputAttributeIndex.get(inputAttributeReference.getAttributeIndex());
    }

    @Nonnull
    @Override
    public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
      return this.getAttributeReference(this.aggregateLevel.getAggregationReference(sourceExpression, context));
    }

    private Reference<?> putAttributeReference(final Reference<?> inputAttributeReference) {
      Reference<?> attributeReference = this.getAttributeReference(inputAttributeReference);
      if (attributeReference == null) {
        this.attributeList.add(inputAttributeReference);
        attributeReference = this.getAttributeReference(this.attributeList.size() - 1);
        this.attributeReferenceMapByInputAttributeIndex.put(
            inputAttributeReference.getAttributeIndex(),
            attributeReference
        );
      }
      return attributeReference;
    }

    private static void buildAttributeList(
        final SemanticAnalyseContext context,
        final List<Evaluation<?>> attributeList,
        final SyntaxTree.NodeList<SQLProject, ?> sourceProjectList,
        final AggregateLevel input
    ) throws SemanticException {
      for (int index = 0; index < sourceProjectList.size(); index++) {
        final SQLExpression sourceProjectExpression = sourceProjectList.get(index).getExpression();
        if (sourceProjectExpression instanceof SQLReference.ColumnAll) {
          attributeList.addAll(input.getAttributeReferences((SQLReference.ColumnAll) sourceProjectExpression));
          continue;
        }
        final Expression<?> targetProject = buildExpression(context, sourceProjectExpression, input);
        if (targetProject instanceof Evaluation) {
          attributeList.add((Evaluation) targetProject);
          continue;
        }
        if (targetProject instanceof Aggregation) {
          attributeList.add(input.getAggregationReference(sourceProjectExpression, context));
          continue;
        }
        throw new UnsupportedOperationException(targetProject.getClass().getName());
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    Relation toRelation(final SemanticAnalyseContext context) throws SemanticException, ExecuteException {
      if (this.aggregateLevel.getGroupBy() != null || this.aggregateLevel.getAggregationList() != null) {
        final ExpressionReconstructor.Context expressionConstructContext = ExpressionReconstructor.Context.get();
        expressionConstructContext.reset(context.getFunctionManager(), this.aggregateLevel);
        try {
          for (int index = 0; index < this.attributeList.size(); index++) {
            final Evaluation<?> attribute = this.attributeList.get(index);
            final Evaluation<?> reconstructedAttribute = (Evaluation<?>) ExpressionReconstructor.INSTANCE.reconstruct(expressionConstructContext, attribute);
            if (reconstructedAttribute != attribute) {
              this.attributeList.set(index, reconstructedAttribute);
            }
          }
        } finally {
          expressionConstructContext.reset(null, null);
        }
        return new LogicalProject(this.aggregateLevel.toRelation(context), this.attributeList);
      } else {
        return new LogicalProject(this.input.toRelation(context), this.attributeList);
      }
    }

    private static final class AggregateLevel extends LogicalTable {

      AggregateLevel(
          final SemanticAnalyseContext context,
          final SQLGroupBy sourceGroupBy,
          final LogicalTable input
      ) throws SemanticException {
        final List<Evaluation<?>> groupBy;
        final LogicalAggregate.GroupType groupType;
        if (sourceGroupBy != null) {
          final SyntaxTree.NodeList<SQLExpression, ?> sourceGroupByExpressionList = sourceGroupBy.getExpressions();
          groupBy = new ArrayList<>(sourceGroupByExpressionList.size());
          for (int index = 0; index < sourceGroupByExpressionList.size(); index++) {
            final SQLExpression sourceGroupByExpression = sourceGroupByExpressionList.get(index);
            final Expression<?> targetGroupByExpression = buildExpression(context, sourceGroupByExpression, input);
            if (!(targetGroupByExpression instanceof Evaluation)) {
              throw new SemanticException(Reason.ILLEGAL_USE_OF_AGGREGATION, sourceGroupByExpression);
            }
            groupBy.add((Evaluation<?>) targetGroupByExpression);
          }
          final SQLGroupType sourceGroupType = sourceGroupBy.getType();
          if (sourceGroupType == null) {
            groupType = null;
          } else if (sourceGroupType instanceof SQLGroupType.ROLLUP) {
            groupType = LogicalAggregate.GroupType.ROLLUP;
          } else if (sourceGroupType instanceof SQLGroupType.CUBE) {
            groupType = LogicalAggregate.GroupType.CUBE;
          } else {
            throw new SemanticException(Reason.UNSUPPORTED_GROUP_TYPE, sourceGroupBy);
          }
        } else {
          groupBy = null;
          groupType = null;
        }
        this.input = input;
        this.groupBy = groupBy;
        this.groupType = groupType;
        this.aggregationList = null;
        this.aggregationReferenceMapByAggregation = null;
      }

      private final LogicalTable input;

      private final List<Evaluation<?>> groupBy;

      private final LogicalAggregate.GroupType groupType;

      private List<Aggregation<?>> aggregationList;

      private List<Reference<?>> aggregationReferences;

      private Map<Aggregation<?>, Reference<?>> aggregationReferenceMapByAggregation;

      private Map<Integer, Reference<?>> aggregationReferenceMapByInputAttributeIndex;

      List<Evaluation<?>> getGroupBy() {
        return this.groupBy;
      }

      @Override
      int getAttributeCount() {
        throw new UnsupportedOperationException();
      }

      @Nonnull
      @Override
      String getAttributeTitle(final int attributeIndex) {
        throw new UnsupportedOperationException();
      }

      @Nonnull
      @Override
      public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
        return this.input.getAttributeReference(columnName);
      }

      @Nonnull
      @Override
      public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
        return this.input.getAttributeReference(sourceReference);
      }

      @Nonnull
      @Override
      public List<Reference<?>> getAttributeReferences() {
        return this.input.getAttributeReferences();
      }

      @Nonnull
      @Override
      public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
        return this.input.getAttributeReferences(sourceReference);
      }

      List<Aggregation<?>> getAggregationList() {
        return this.aggregationList;
      }

      @Nonnull
      @Override
      public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
        final Expression<?> expression = buildExpression(context, sourceExpression, this.input);
        if (!(expression instanceof Aggregation)) {
          throw new RuntimeException(expression.toString());
        }
        return this.getAggregationReference((Aggregation<?>) expression);
      }

      Reference<?> getAggregationReference(final Reference<?> inputAttributeReference) {
        if (this.aggregationReferenceMapByInputAttributeIndex == null) {
          this.aggregationReferenceMapByInputAttributeIndex = new HashMap<>();
        }
        Reference<?> aggregationReference = this.aggregationReferenceMapByInputAttributeIndex.get(inputAttributeReference.getAttributeIndex());
        if (aggregationReference == null) {
          aggregationReference = this.getAggregationReference(new FirstExpression<>(inputAttributeReference));
          this.aggregationReferenceMapByInputAttributeIndex.put(inputAttributeReference.getAttributeIndex(), aggregationReference);
        }
        return aggregationReference;
      }

      List<Reference<?>> getAggregationReferences() {
        return this.aggregationReferences;
      }

      private Reference<?> getAggregationReference(final Aggregation<?> aggregation) {
        if (this.aggregationList == null) {
          this.aggregationList = new ArrayList<>();
        }
        if (this.aggregationReferences == null) {
          this.aggregationReferences = new ArrayList<>();
        }
        if (this.aggregationReferenceMapByAggregation == null) {
          this.aggregationReferenceMapByAggregation = new HashMap<>();
        }
        Reference<?> aggregationReference = this.aggregationReferenceMapByAggregation.get(aggregation);
        if (aggregationReference == null) {
          final int aggregationIndex = this.aggregationList.size();
          aggregationReference = ReferenceFactory.of(aggregationIndex, aggregation.getResultClass(), aggregation.isNullable());
          this.aggregationList.add(aggregation);
          this.aggregationReferences.add(aggregationReference);
          this.aggregationReferenceMapByAggregation.put(aggregation, aggregationReference);
        }
        return aggregationReference;
      }

      @Override
      Relation toRelation(final SemanticAnalyseContext context) throws SemanticException, ExecuteException {
        if (this.aggregationList.isEmpty()) {
          throw new RuntimeException();
        }
        return new com.huawei.cloudtable.leo.relations.LogicalAggregate(
            this.input.toRelation(context),
            this.aggregationList,
            this.groupBy,
            this.groupType
        );
      }

    }

    private static final class ExpressionReconstructor extends com.huawei.cloudtable.leo.ExpressionReconstructor<ExpressionReconstructor.Context> {

      static final ExpressionReconstructor INSTANCE = new ExpressionReconstructor();

      @Override
      public Evaluation visit(final Reference<?> reference, final Context context) {
        final AggregateLevel aggregate = context.getAggregateLevel();
        final List<Reference<?>> aggregationReferences = aggregate.getAggregationReferences();
        if (aggregationReferences == null
          || reference.getAttributeIndex() >= aggregationReferences.size()
          || aggregationReferences.get(reference.getAttributeIndex()) != reference) {
          return aggregate.getAggregationReference(reference);
        } else {
          return reference;
        }
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

        private AggregateLevel aggregateLevel;

        @Override
        public FunctionManager getFunctionManager() {
          return this.functionManager;
        }

        AggregateLevel getAggregateLevel() {
          return this.aggregateLevel;
        }

        void reset(final FunctionManager functionManager, final AggregateLevel aggregateLevel) {
          this.functionManager = functionManager;
          this.aggregateLevel = aggregateLevel;
        }

      }

    }

  }

  private static final class LogicalHaving extends LogicalFilter {

    LogicalHaving(
        final SemanticAnalyseContext context,
        final SQLHaving sourceHaving,
        final LogicalSelect logicalSelect
    ) throws SemanticException {
      super(context, sourceHaving.getCondition(), logicalSelect);
      // TODO 聚合运算，要下放到Select去执行
    }

  }

  private static final class LogicalOffset extends LogicalTable {

    LogicalOffset(final SQLOffset sourceOffset, final LogicalTable input) {
      this.offset = sourceOffset.getValue();
      this.input = input;
    }

    private final long offset;

    private final LogicalTable input;

    @Override
    int getAttributeCount() {
      return this.input.getAttributeCount();
    }

    @Nonnull
    @Override
    String getAttributeTitle(final int attributeIndex) {
      return this.input.getAttributeTitle(attributeIndex);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
      return this.input.getAttributeReference(columnName);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
      return this.input.getAttributeReference(sourceReference);
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences() {
      return this.input.getAttributeReferences();
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
      return this.input.getAttributeReferences(sourceReference);
    }

    @Nonnull
    @Override
    public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
      return this.input.getAggregationReference(sourceExpression, context);
    }

    @Override
    Relation toRelation(final SemanticAnalyseContext context) throws SemanticException, ExecuteException {
      return new com.huawei.cloudtable.leo.relations.LogicalLimit(this.input.toRelation(context), this.offset);
    }

  }

  private static final class LogicalLimit extends LogicalTable {

    LogicalLimit(final SQLLimit sourceLimit, final LogicalTable input) {
      this.limit = sourceLimit.getValue();
      this.input = input;
    }

    private final long limit;

    private final LogicalTable input;

    @Override
    int getAttributeCount() {
      return this.input.getAttributeCount();
    }

    @Nonnull
    @Override
    String getAttributeTitle(final int attributeIndex) {
      return this.input.getAttributeTitle(attributeIndex);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final Identifier columnName) throws SemanticException {
      return this.input.getAttributeReference(columnName);
    }

    @Nonnull
    @Override
    public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
      return this.input.getAttributeReference(sourceReference);
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences() {
      return this.input.getAttributeReferences();
    }

    @Nonnull
    @Override
    public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
      return this.input.getAttributeReferences(sourceReference);
    }

    @Nonnull
    @Override
    public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
      return this.input.getAggregationReference(sourceExpression, context);
    }

    @Override
    Relation toRelation(final SemanticAnalyseContext context) throws SemanticException, ExecuteException {
      return new com.huawei.cloudtable.leo.relations.LogicalLimit(this.input.toRelation(context), this.limit);
    }

  }

}
