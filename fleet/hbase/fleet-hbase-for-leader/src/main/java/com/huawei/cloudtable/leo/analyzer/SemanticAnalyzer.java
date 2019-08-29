package com.huawei.cloudtable.leo.analyzer;

import com.huawei.cloudtable.leo.ExecuteException;
import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.FunctionIdentifiers;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.Statement;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.expression.Aggregation;
import com.huawei.cloudtable.leo.expression.Case;
import com.huawei.cloudtable.leo.expression.Constant;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;
import com.huawei.cloudtable.leo.expression.FunctionAmbiguousException;
import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.expression.LikeExpression;
import com.huawei.cloudtable.leo.expression.LogicalAnd;
import com.huawei.cloudtable.leo.expression.LogicalOr;
import com.huawei.cloudtable.leo.expression.LogicalXor;
import com.huawei.cloudtable.leo.expression.Reference;
import com.huawei.cloudtable.leo.language.Lexical;
import com.huawei.cloudtable.leo.language.SQLStatement;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.expression.SQLAdditionExpression;
import com.huawei.cloudtable.leo.language.expression.SQLAndExpression;
import com.huawei.cloudtable.leo.language.expression.SQLBetweenExpression;
import com.huawei.cloudtable.leo.language.expression.SQLCaseExpression;
import com.huawei.cloudtable.leo.language.expression.SQLConstant;
import com.huawei.cloudtable.leo.language.expression.SQLDivisionExpression;
import com.huawei.cloudtable.leo.language.expression.SQLEqualsExpression;
import com.huawei.cloudtable.leo.language.expression.SQLFunctionExpression;
import com.huawei.cloudtable.leo.language.expression.SQLGreaterExpression;
import com.huawei.cloudtable.leo.language.expression.SQLGreaterOrEqualsExpression;
import com.huawei.cloudtable.leo.language.expression.SQLInExpression;
import com.huawei.cloudtable.leo.language.expression.SQLIsNullExpression;
import com.huawei.cloudtable.leo.language.expression.SQLLessExpression;
import com.huawei.cloudtable.leo.language.expression.SQLLessOrEqualsExpression;
import com.huawei.cloudtable.leo.language.expression.SQLLikeExpression;
import com.huawei.cloudtable.leo.language.expression.SQLModuloExpression;
import com.huawei.cloudtable.leo.language.expression.SQLMultiplicationExpression;
import com.huawei.cloudtable.leo.language.expression.SQLNotEqualsExpression;
import com.huawei.cloudtable.leo.language.expression.SQLNotExpression;
import com.huawei.cloudtable.leo.language.expression.SQLNull;
import com.huawei.cloudtable.leo.language.expression.SQLOrExpression;
import com.huawei.cloudtable.leo.language.expression.SQLPreferentialExpression;
import com.huawei.cloudtable.leo.language.expression.SQLReference;
import com.huawei.cloudtable.leo.language.expression.SQLSubtractionExpression;
import com.huawei.cloudtable.leo.language.expression.SQLVariable;
import com.huawei.cloudtable.leo.language.expression.SQLXorExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import javafx.util.Pair;

import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public abstract class SemanticAnalyzer<TSourceStatement extends SQLStatement, TTargetStatement extends Statement> {

  protected static Expression buildExpression(
      final SemanticAnalyseContext context,
      final SQLExpression sourceExpression
  ) throws SemanticException {
    return buildExpression(context, sourceExpression, ReferenceResolver.DEFAULT);
  }

  protected static Expression buildExpression(
      final SemanticAnalyseContext context,
      final SQLExpression sourceExpression,
      final ReferenceResolver referenceResolver
  ) throws SemanticException {
    final ExpressionBuildContext buildContext = ExpressionBuildContext.get(context, referenceResolver);
    try {
      return ExpressionBuilder.build(buildContext, sourceExpression);
    } finally {
      buildContext.reset(null, null);
    }
  }

  protected static Expression<?> buildExpression(
      final SemanticAnalyseContext context,
      final Identifier functionName,
      final Evaluation<?>... functionParameters
  ) throws SemanticException, ExecuteException {
    return buildExpression(context.getFunctionManager(), functionName, functionParameters);
  }

  protected static Expression<?> buildExpression(
      final FunctionManager functionManager,
      final Identifier functionName,
      final Evaluation<?>... functionParameters
  ) throws SemanticException {
    return ExpressionBuilder.build(functionManager, functionName, functionParameters);
  }

  public SemanticAnalyzer(final Class<TSourceStatement> sourceClass, final Class<TTargetStatement> targetClass) {
    if (sourceClass == null) {
      throw new IllegalArgumentException("Argument [sourceClass] is null.");
    }
    if (targetClass == null) {
      throw new IllegalArgumentException("Argument [targetClass] is null.");
    }
    this.sourceClass = sourceClass;
    this.targetClass = targetClass;
  }

  private final Class<TSourceStatement> sourceClass;

  private final Class<TTargetStatement> targetClass;

  public Class<TSourceStatement> getSourceClass() {
    return this.sourceClass;
  }

  public Class<TTargetStatement> getTargetClass() {
    return this.targetClass;
  }

  public abstract TTargetStatement analyse(SemanticAnalyseContext context, TSourceStatement sourceStatement) throws SyntaxException, SemanticException, ExecuteException;

  public static final class ReferenceFactory {

    @SuppressWarnings("unchecked")
    public static <TValue> Reference<TValue> of(
        final int attributeIndex,
        final Class<TValue> attributeClass,
        final boolean attributeNullable
    ) {
      if (attributeIndex > CACHE_SIZE) {
        return new Reference<>(attributeIndex, attributeClass, attributeNullable);
      }
      final ValueType<TValue> attributeValueType = ValueTypeManager.BUILD_IN.getValueType(attributeClass);
      if (attributeValueType == null) {
        return new Reference<>(attributeIndex, attributeClass, attributeNullable);
      } else {
        Map<Class<?>, ReferencePair<?>> cache = CACHE[attributeIndex];
        if (cache == null) {
          synchronized (CACHE) {
            cache = CACHE[attributeIndex];
            if (cache == null) {
              cache = new HashMap<>();
              cache.put(attributeClass, new ReferencePair<>(attributeIndex, attributeClass));
              CACHE[attributeIndex] = cache;
            }
          }
        }
        ReferencePair<TValue> referencePair = (ReferencePair<TValue>) cache.get(attributeClass);
        if (referencePair == null) {
          synchronized (CACHE) {
            cache = CACHE[attributeIndex];
            referencePair = (ReferencePair<TValue>) cache.get(attributeClass);
            if (referencePair == null) {
              referencePair = new ReferencePair<>(attributeIndex, attributeClass);
              cache = new HashMap<>(cache);
              cache.put(attributeClass, referencePair);
              CACHE[attributeIndex] = cache;
            }
          }
        }
        return referencePair.get(attributeNullable);
      }
    }

    private static final int CACHE_SIZE = 256;

    @SuppressWarnings("unchecked")
    private static final Map<Class<?>, ReferencePair<?>>[] CACHE = new Map[CACHE_SIZE];

    private static final class ReferencePair<TValue> {

      ReferencePair(final int attributeIndex, final Class<TValue> attributeClass) {
        this.referenceNullable = new Reference<>(attributeIndex, attributeClass, true);
        this.referenceNonNullable = new Reference<>(attributeIndex, attributeClass, false);
      }

      private final Reference<TValue> referenceNullable;

      private final Reference<TValue> referenceNonNullable;

      Reference<TValue> get(final boolean nullable) {
        return nullable ? this.referenceNullable : this.referenceNonNullable;
      }

    }

  }

  public static abstract class ReferenceResolver {

    private static final ReferenceResolver DEFAULT = new ReferenceResolver() {
      @Nonnull
      @Override
      public Reference<?> getAttributeReference(final SQLReference.Column sourceReference) throws SemanticException {
        throw new SemanticException(SemanticException.Reason.ILLEGAL_USE_OF_REFERENCE, sourceReference);
      }

      @Nonnull
      @Override
      public List<Reference<?>> getAttributeReferences(final SQLReference.ColumnAll sourceReference) throws SemanticException {
        throw new SemanticException(SemanticException.Reason.ILLEGAL_USE_OF_REFERENCE, sourceReference);
      }

      @Nonnull
      @Override
      public Reference<?> getAggregationReference(final SQLExpression sourceExpression, final SemanticAnalyseContext context) throws SemanticException {
        throw new SemanticException(SemanticException.Reason.ILLEGAL_USE_OF_AGGREGATION, null);
      }
    };

    @Nonnull
    public abstract Reference<?> getAttributeReference(SQLReference.Column sourceReference) throws SemanticException;

    @Nonnull
    public abstract List<Reference<?>> getAttributeReferences(SQLReference.ColumnAll sourceReference) throws SemanticException;

    /**
     * 方法的实现里面要判断aggregation的合法性
     */
    @Nonnull
    public abstract Reference<?> getAggregationReference(SQLExpression sourceExpression, SemanticAnalyseContext context) throws SemanticException;

  }

  private static final class ExpressionBuilder implements SQLExpressionVisitor<Expression, ExpressionBuildContext> {

    private static final ExpressionBuilder INSTANCE = new ExpressionBuilder();

    @Nonnull
    static List<Reference<?>> build(
        final ExpressionBuildContext context,
        final SQLReference.ColumnAll sourceReference
    ) throws SemanticException {
      return context.getReferenceResolver().getAttributeReferences(sourceReference);
    }

    @Nonnull
    static Expression build(final ExpressionBuildContext context, final SQLExpression sourceExpression) throws SemanticException {
      try {
        return sourceExpression.accept(INSTANCE, context);
      } catch (UndeclaredThrowableException exception) {
        if (exception.getUndeclaredThrowable() != null
            && exception.getUndeclaredThrowable() instanceof SemanticException) {
          throw (SemanticException) exception.getUndeclaredThrowable();
        } else {
          throw exception;
        }
      }
    }

    @Nonnull
    static Expression<?> build(
        final FunctionManager functionManager,
        final Identifier functionName,
        final Evaluation<?>... functionParameters
    ) throws SemanticException {
      final Function.Declare<?, ?> functionDeclare;
      try {
        functionDeclare = functionManager.getFunctionDeclare(functionName, functionParameters);
      } catch (FunctionAmbiguousException exception) {
        throw new SemanticException(SemanticException.Reason.FUNCTION_AMBIGUOUS, null, functionName.toString() /* TODO + functionParameters*/);
      }
      if (functionDeclare == null) {
        throw new SemanticException(SemanticException.Reason.FUNCTION_NOT_FOUND, null, functionName.toString());
      }
      // Check argument classes. // TODO 在获取FunctionDeclare后，参数的隐式转换关系就应该已经确认好了，不需要重复检查
      int functionParameterIndex = 0;
      for (; functionParameterIndex < functionDeclare.getValueParameterCount(); functionParameterIndex++) {
        if (functionParameters[functionParameterIndex] == null) {
          continue;
        }
        final Class<?> expectedParameterClass = functionDeclare.getValueParameterDeclare(functionParameterIndex).getClazz();
        final Class<?> actualParameterClass = functionParameters[functionParameterIndex].getResultClass();
        if (!expectedParameterClass.isAssignableFrom(actualParameterClass)) {
          final Evaluation<?> actualParameter = functionManager.buildCastingFunction(
              functionParameters[functionParameterIndex],
              expectedParameterClass
          );
          if (actualParameter == null) {
            // TODO
            throw new UnsupportedOperationException("Invalid value. ");
          } else {
            functionParameters[functionParameterIndex] = actualParameter;
          }
        }
      }
      if (functionDeclare.getArrayParameterDeclare() != null) {
        final Function.ArrayParameterDeclare arrayParameterDeclare = functionDeclare.getArrayParameterDeclare();
        for (; functionParameterIndex < functionParameters.length; functionParameterIndex++) {
          if (functionParameters[functionParameterIndex] == null) {
            continue;
          }
          final Class<?> actualParameterClass = functionParameters[functionParameterIndex].getResultClass();
          if (!arrayParameterDeclare.getElementClass().isAssignableFrom(actualParameterClass)) {
            final Evaluation<?> actualParameter = functionManager.buildCastingFunction(
                functionParameters[functionParameterIndex],
                arrayParameterDeclare.getElementClass()
            );
            if (actualParameter == null) {
              // TODO
              throw new UnsupportedOperationException("Invalid value. ");
            } else {
              functionParameters[functionParameterIndex] = actualParameter;
            }
          }
        }
      }
      return functionDeclare.newInstance(functionParameters);
    }

    private static Evaluation<?> buildEvaluation(
        final ExpressionBuildContext context,
        final SQLExpression sourceExpression
    ) throws SemanticException {
      final Expression targetExpression = build(context, sourceExpression);
      if (targetExpression instanceof Evaluation) {
        return (Evaluation) targetExpression;
      }
      if (targetExpression instanceof Aggregation) {
        final SemanticAnalyseContext semanticAnalyseContext = context.getSemanticAnalyseContext();
        final ReferenceResolver referenceResolver = context.getReferenceResolver();
        try {
          // 调用外部逻辑，context可能会被重置，调用完后，需要对现场进行复原
          return context.getReferenceResolver().getAggregationReference(sourceExpression, context.getSemanticAnalyseContext());
        } finally {
          context.reset(semanticAnalyseContext, referenceResolver);
        }
      }
      throw new UnsupportedOperationException();
    }

    private ExpressionBuilder() {
      // to do nothing.
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression visit(final SQLAndExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        if (targetParameter1.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        if (targetParameter2.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        return new LogicalAnd(targetParameter1, targetParameter2);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLBetweenExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetValueParameter = buildEvaluation(context, sourceExpression.getValueParameter());
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetValueParameter, targetParameter1, targetParameter2};// TODO 考虑数组复用
        return build(context.getFunctionManager(), FunctionIdentifiers.BETWEEN_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression visit(final SQLOrExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        if (targetParameter1.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        if (targetParameter2.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        return new LogicalOr(targetParameter1, targetParameter2);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLPreferentialExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        return build(context, sourceExpression.getExpression());
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLConstant sourceExpression, final ExpressionBuildContext context) {
      if (sourceExpression instanceof SQLConstant.String) {
        final String string = ((SQLConstant.String) sourceExpression).getValue();
        return new Constant<>(String.class, string, string);
      }
      if (sourceExpression instanceof SQLConstant.Integer) {
        final String number = ((SQLConstant.Integer) sourceExpression).getValue();
        // INTEGER.
        try {
          return new Constant<>(Byte.class, Byte.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(Short.class, Short.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(Integer.class, Integer.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(Long.class, Long.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(BigInteger.class, new BigInteger(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        // TODO 抛异常，NumberFormatException
        throw new UnsupportedOperationException();
      } else if (sourceExpression instanceof SQLConstant.Boolean) {
        final String value = ((SQLConstant.Boolean) sourceExpression).getValue();
        return new Constant<>(Boolean.class, Boolean.valueOf(value));
      } else if (sourceExpression instanceof SQLConstant.Real) {
        final String number = ((SQLConstant.Real) sourceExpression).getValue();
        try {
          return new Constant<>(Float.class, Float.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(Double.class, Double.valueOf(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        try {
          return new Constant<>(BigDecimal.class, new BigDecimal(number), number);
        } catch (NumberFormatException ignore) {
          // to do nothing.
        }
        // TODO 抛异常，NumberFormatException
        throw new UnsupportedOperationException();
      }
      throw new RuntimeException(sourceExpression.getClass().getName());
    }

    @Override
    public Expression visit(final SQLEqualsExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.EQUALS_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression visit(final SQLXorExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        if (targetParameter1.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        if (targetParameter2.getResultClass() != Boolean.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        return new LogicalXor(targetParameter1, targetParameter2);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Expression visit(final SQLLikeExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetValueParameter = buildEvaluation(context, sourceExpression.getValueParameter());
        final Evaluation targetPatternParameter = buildEvaluation(context, sourceExpression.getPatternParameter());
        if (targetValueParameter.getResultClass() != String.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        if (targetPatternParameter.getResultClass() != String.class) {
          // TODO
          throw new UnsupportedOperationException();
        }
        return new LikeExpression(targetValueParameter, targetPatternParameter);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    public Expression visit(final SQLCaseExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        SyntaxTree.NodeList<SQLCaseExpression.Branch, Lexical.Whitespace> branches = sourceExpression.getBranches();
        List<Case.Branch<Evaluation>> caseBranches = new ArrayList<Case.Branch<Evaluation>>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
          SQLCaseExpression.Branch branch = branches.get(i);
          if (branch.getCondition() == null) {
            throw new UnsupportedOperationException("the condition should not be empty");
          }
          final Evaluation condition = buildEvaluation(context, branch.getCondition());

          Evaluation operate = null;
          if (!(branch.getOperate() instanceof SQLNull)) {
            operate = buildEvaluation(context, branch.getOperate());
          }

          Case.Branch<Evaluation> caseBranch = new Case.Branch(condition, operate);
          caseBranches.add(caseBranch);
        }

        if (caseBranches.isEmpty()) {
          throw new UnsupportedOperationException("the case condition should not be empty");
        }

        Evaluation targetOperateElse = null;
        if (sourceExpression.getOperateElse() != null &&
          !(sourceExpression.getOperateElse() instanceof SQLNull)) {
          targetOperateElse = buildEvaluation(context, sourceExpression.getOperateElse());
        }

        return new Case(caseBranches, targetOperateElse);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLModuloExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.MODULO_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLFunctionExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final SyntaxTree.NodeList<SQLExpression, ?> sourceFunctionParameters = sourceExpression.getParameters();
        boolean haveReferenceAll = false;
        for (int index = 0; index < sourceFunctionParameters.size(); index++) {
          final SQLExpression sourceFunctionParameter = sourceFunctionParameters.get(index);
          if (sourceFunctionParameter instanceof SQLReference.ColumnAll) {
            haveReferenceAll = true;
            break;
          }
        }
        final Evaluation<?>[] targetFunctionParameters;
        if (haveReferenceAll) {
          List<Evaluation<?>> targetFunctionParameterList = new ArrayList<>(sourceFunctionParameters.size());
          for (int index = 0; index < sourceFunctionParameters.size(); index++) {
            final SQLExpression sourceFunctionParameter = sourceFunctionParameters.get(index);
            if (sourceFunctionParameter instanceof SQLReference.ColumnAll) {
              targetFunctionParameterList.addAll(build(context, (SQLReference.ColumnAll) sourceFunctionParameter));
            } else {
              targetFunctionParameterList.add(buildEvaluation(context, sourceFunctionParameter));
            }
          }
          targetFunctionParameters = targetFunctionParameterList.toArray(Evaluation.EMPTY_LIST);
        } else {
          targetFunctionParameters = new Evaluation<?>[sourceFunctionParameters.size()];
          for (int index = 0; index < sourceFunctionParameters.size(); index++) {
            targetFunctionParameters[index] = buildEvaluation(context, sourceFunctionParameters.get(index));
          }
        }
        final Identifier targetFunctionIdentifier = sourceExpression.getName().getValue();
        return build(context.getFunctionManager(), targetFunctionIdentifier, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLGreaterExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.GREATER_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLGreaterOrEqualsExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.GREATER_OR_EQUALS_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLInExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final SQLExpression sourceValueParameter = sourceExpression.getValueParameter();
        final SyntaxTree.NodeList<SQLExpression.Priority3, ?> sourceArrayParameters = sourceExpression.getParameters();
        boolean haveReferenceAll = false;
        if (sourceValueParameter instanceof SQLReference.ColumnAll) {
          // TODO 抛语法错误
          throw new UnsupportedOperationException();
        }
        for (int index = 0; index < sourceArrayParameters.size(); index++) {
          final SQLExpression sourceFunctionParameter = sourceArrayParameters.get(index);
          if (sourceFunctionParameter instanceof SQLReference.ColumnAll) {
            haveReferenceAll = true;
            break;
          }
        }
        final Evaluation<?>[] targetFunctionParameters;
        if (haveReferenceAll) {
          List<Evaluation<?>> targetFunctionParameterList = new ArrayList<>(1 + sourceArrayParameters.size());
          targetFunctionParameterList.add(buildEvaluation(context, sourceValueParameter));
          for (int index = 0; index < sourceArrayParameters.size(); index++) {
            final SQLExpression sourceFunctionParameter = sourceArrayParameters.get(index);
            if (sourceFunctionParameter instanceof SQLReference.ColumnAll) {
              targetFunctionParameterList.addAll(build(context, (SQLReference.ColumnAll) sourceFunctionParameter));
            } else {
              targetFunctionParameterList.add(buildEvaluation(context, sourceFunctionParameter));
            }
          }
          targetFunctionParameters = targetFunctionParameterList.toArray(Evaluation.EMPTY_LIST);
        } else {
          targetFunctionParameters = new Evaluation<?>[1 + sourceArrayParameters.size()];
          targetFunctionParameters[0] = buildEvaluation(context, sourceValueParameter);
          for (int index = 0; index < sourceArrayParameters.size(); index++) {
            targetFunctionParameters[1 + index] = buildEvaluation(context, sourceArrayParameters.get(index));
          }
        }
        final Identifier targetFunctionIdentifier = FunctionIdentifiers.IN_FUNCTION_IDENTIFIER;
        return build(context.getFunctionManager(), targetFunctionIdentifier, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLIsNullExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetValueParameter = buildEvaluation(context, sourceExpression.getValueParameter());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetValueParameter};
        return build(context.getFunctionManager(), FunctionIdentifiers.IS_NULL_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLLessExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.LESS_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLLessOrEqualsExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetParameter2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter1, targetParameter2};
        return build(context.getFunctionManager(), FunctionIdentifiers.LESS_OR_EQUALS_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLNull sourceExpression, final ExpressionBuildContext context) {
      return null;
    }

    @Override
    public Expression visit(final SQLReference.Column sourceExpression, final ExpressionBuildContext context) {
      try {
        return context.getReferenceResolver().getAttributeReference(sourceExpression);
      } catch (SemanticException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLReference.ColumnAll sourceExpression, final ExpressionBuildContext context) {
      throw new RuntimeException();// 应当在语法分析或者外层语义分析时规避。
    }

    @Override
    public Expression visit(final SQLVariable sourceExpression, final ExpressionBuildContext context) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public Expression visit(final SQLNotExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetParameter = buildEvaluation(context, sourceExpression.getParameter());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[]{targetParameter};
        return build(context.getFunctionManager(), FunctionIdentifiers.NOT_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLAdditionExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetFunctionParameters1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetFunctionParameters2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[] {targetFunctionParameters1, targetFunctionParameters2};
        return build(context.getFunctionManager(), FunctionIdentifiers.ADDITION_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLSubtractionExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetFunctionParameters1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetFunctionParameters2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[] {targetFunctionParameters1, targetFunctionParameters2};
        return build(context.getFunctionManager(), FunctionIdentifiers.SUBTRACTION_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLMultiplicationExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetFunctionParameters1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetFunctionParameters2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[] {targetFunctionParameters1, targetFunctionParameters2};
        return build(context.getFunctionManager(), FunctionIdentifiers.MULTIPLICATION_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLNotEqualsExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetFunctionParameters1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetFunctionParameters2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[] {targetFunctionParameters1, targetFunctionParameters2};
        return build(context.getFunctionManager(), FunctionIdentifiers.NOT_EQUALS_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

    @Override
    public Expression visit(final SQLDivisionExpression sourceExpression, final ExpressionBuildContext context) {
      try {
        final Evaluation targetFunctionParameters1 = buildEvaluation(context, sourceExpression.getParameter1());
        final Evaluation targetFunctionParameters2 = buildEvaluation(context, sourceExpression.getParameter2());
        final Evaluation<?>[] targetFunctionParameters = new Evaluation<?>[] {targetFunctionParameters1, targetFunctionParameters2};
        return build(context.getFunctionManager(), FunctionIdentifiers.DIVISION_FUNCTION_IDENTIFIER, targetFunctionParameters);
      } catch (SemanticException | ExecuteException exception) {
        throw new UndeclaredThrowableException(exception);
      }
    }

  }

  private static final class ExpressionBuildContext {

    static ExpressionBuildContext get(final SemanticAnalyseContext semanticAnalyseContext, final ReferenceResolver referenceResolver) {
      final ExpressionBuildContext context = CONTEXT_CACHE.get();
      context.reset(semanticAnalyseContext, referenceResolver);
      return context;
    }

    private static final ThreadLocal<ExpressionBuildContext> CONTEXT_CACHE = new ThreadLocal<ExpressionBuildContext>() {
      @Override
      protected ExpressionBuildContext initialValue() {
        return new ExpressionBuildContext();
      }
    };

    private ExpressionBuildContext() {
      // to do nothing.
    }

    private SemanticAnalyseContext semanticAnalyseContext;

    private ReferenceResolver referenceResolver;

    private void reset(final SemanticAnalyseContext semanticAnalyseContext, final ReferenceResolver referenceResolver) {
      this.semanticAnalyseContext = semanticAnalyseContext;
      this.referenceResolver = referenceResolver;
    }

    SemanticAnalyseContext getSemanticAnalyseContext() {
      return this.semanticAnalyseContext;
    }

    @Nonnull
    FunctionManager getFunctionManager() throws ExecuteException {
      return this.semanticAnalyseContext.getFunctionManager();
    }

    @Nonnull
    ReferenceResolver getReferenceResolver() {
      return this.referenceResolver;
    }

  }

}
