package com.huawei.cloudtable.leo.analyzers;

import com.huawei.cloudtable.leo.ExecuteContext;
import com.huawei.cloudtable.leo.ExecuteException;
import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyseContext;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyzer;
import com.huawei.cloudtable.leo.analyzer.SemanticException;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.expression.Variable;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLIdentifier;
import com.huawei.cloudtable.leo.language.statement.SQLTableIdentifier;
import com.huawei.cloudtable.leo.language.statement.SQLValues;
import com.huawei.cloudtable.leo.language.statements.SQLInsertStatement;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.metadata.TableReference;
import com.huawei.cloudtable.leo.statements.InsertStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class InsertAnalyzer<TSourceStatement extends SQLInsertStatement, TTargetStatement extends InsertStatement>
    extends SemanticAnalyzer<TSourceStatement, TTargetStatement> {

  private InsertAnalyzer(final Class<TSourceStatement> sourceClass, final Class<TTargetStatement> targetClass) {
    super(sourceClass, targetClass);
  }

  public static final class FromValues extends InsertAnalyzer<SQLInsertStatement.FromValues, InsertStatement.FromValues> {

    public FromValues() {
      super(SQLInsertStatement.FromValues.class, InsertStatement.FromValues.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InsertStatement.FromValues analyse(
        final SemanticAnalyseContext context,
        final SQLInsertStatement.FromValues sourceStatement
    ) throws SemanticException, ExecuteException {
      final ExecuteContext executeContext = context.getExecuteContext();
      final SQLTableIdentifier tableIdentifier = sourceStatement.getTable();
      final TableReference tableReference;
      try {
        if (tableIdentifier.getSchemaName() == null) {
          tableReference = context.getExecuteEngine().getTableReference(
              executeContext,
              tableIdentifier.getTableName().getValue()
          );
        } else {
          tableReference = context.getExecuteEngine().getTableReference(
              executeContext,
              tableIdentifier.getSchemaName().getValue(),
              tableIdentifier.getTableName().getValue()
          );
        }
      } catch (ExecuteException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
      if (tableReference == null) {
        // TODO Table not found.
        throw new UnsupportedOperationException();
      }
      final List<TableDefinition.Column<?>> tableColumnList;
      if (sourceStatement.getColumns() == null) {
        tableColumnList = tableReference.getColumnList();
      } else {
        final SyntaxTree.NodeList<SQLIdentifier, ?> sourceTableColumnList = sourceStatement.getColumns();
        tableColumnList = new ArrayList<>(sourceTableColumnList.size());
        final Set<Identifier> tableColumnNameSet = new HashSet<>(sourceTableColumnList.size());
        for (int index = 0; index < sourceTableColumnList.size(); index++) {
          final SQLIdentifier sourceTableColumn = sourceTableColumnList.get(index);
          final Identifier tableColumnName = sourceTableColumn.getValue();
          if (tableColumnNameSet.contains(tableColumnName)) {
            // TODO
            throw new UnsupportedOperationException();
          }
          final TableDefinition.Column<?> tableColumn = tableReference.getColumn(tableColumnName);
          if (tableColumn == null) {
            throw new SemanticException(SemanticException.Reason.COLUMN_NOT_FOUND, sourceTableColumn);
          }
          tableColumnList.add(tableColumn);
          tableColumnNameSet.add(tableColumnName);
        }
      }
      final FunctionManager functionManager = context.getFunctionManager();
      final SyntaxTree.NodeList<SQLValues, ?> sourceValuesList = sourceStatement.getSource();
      final List<Evaluation<?>[]> targetValuesList = new ArrayList<>(sourceValuesList.size());
      for (int index = 0; index < sourceValuesList.size(); index++) {
        final SyntaxTree.NodeList<SQLExpression, ?> sourceValues = sourceValuesList.get(index).getValues();
        final Evaluation<?>[] targetValues = new Evaluation[sourceValues.size()];
        for (int valueIndex = 0; valueIndex < sourceValues.size(); valueIndex++) {
          Expression<?> targetValue = buildExpression(context, sourceValues.get(valueIndex));
          if (!(targetValue instanceof Evaluation)) {
            // TODO
            throw new UnsupportedOperationException();
          }
          if (!(targetValue instanceof Variable)) {
            final TableDefinition.Column column = tableColumnList.get(valueIndex);
            if (!column.getValueClass().isAssignableFrom(targetValue.getResultClass())) {
              targetValue = functionManager.buildCastingFunction((Evaluation) targetValue, column.getValueClass());
              if (targetValue == null) {
                // TODO
                throw new UnsupportedOperationException("Invalid value, value index is " + valueIndex);
              }
            }
          }
          targetValues[valueIndex] = (Evaluation) targetValue;
        }
        targetValuesList.add(targetValues);
      }
      return new InsertStatement.FromValues(
          tableReference,
          tableColumnList,
          InsertStatement.DuplicateKeyPolicy.REPLACE,// TODO default
          targetValuesList
      );
    }

  }

}
