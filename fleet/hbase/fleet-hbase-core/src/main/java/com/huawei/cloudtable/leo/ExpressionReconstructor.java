package com.huawei.cloudtable.leo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.huawei.cloudtable.leo.expression.Case;
import com.huawei.cloudtable.leo.expression.Casting;
import com.huawei.cloudtable.leo.expression.Constant;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.expression.Function;
import com.huawei.cloudtable.leo.expression.FunctionAmbiguousException;
import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.expression.LikeExpression;
import com.huawei.cloudtable.leo.expression.LogicalAnd;
import com.huawei.cloudtable.leo.expression.LogicalNot;
import com.huawei.cloudtable.leo.expression.LogicalOr;
import com.huawei.cloudtable.leo.expression.LogicalXor;
import com.huawei.cloudtable.leo.expression.Reference;
import com.huawei.cloudtable.leo.expression.Variable;

/**
 * 表达式重构器。
 * 用于SQL分析和优化过程中，需要对表达式进行精简和重构的场景，如常量折叠等。
 * 当前类的默认实现是不对表达式执行任何重构动作，需要在具体的应用场景中有针对性的覆写部分方法。
 * @param <TContext>
 * @since 2019-07-25
 */
public class ExpressionReconstructor<TContext extends ExpressionReconstructor.Context> extends ExpressionVisitor<Expression, TContext> {

  private static final int FUNCTION_PARAMETERS_TEMPLATE_CACHE_SIZE = 10;

  private static final ThreadLocal<Map<Integer, Evaluation[]>> FUNCTION_PARAMETERS_TEMPLATE_CACHE = new ThreadLocal<Map<Integer, Evaluation[]>>() {
    @Override
    protected Map<Integer, Evaluation[]> initialValue() {
      return new HashMap<>();
    }
  };

  @SuppressWarnings("unchecked")
  public <TResult> Expression<TResult> reconstruct(final TContext context, final Expression<TResult> expression) {
    return (Expression<TResult>) this.reconstruct0(context, expression);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final Casting<?, ?> casting, final TContext context) {
    final Evaluation<?> parameter = casting.getSource();
    final Evaluation<?> reconstructedParameter = this.reconstruct0(context, parameter);
    if (reconstructedParameter != parameter) {
      return new Casting(reconstructedParameter, casting.getConverter());
    }
    return casting;
  }

  @Override
  public Evaluation visit(final Constant<?> constant, final TContext context) {
    return constant;
  }

  @Override
  public Evaluation visit(final Reference<?> reference, final TContext context) {
    return reference;
  }

  @Override
  public Evaluation visit(final Variable<?> variable, final TContext context) {
    return variable;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final LikeExpression like, final TContext context) {
    final Evaluation<?> valueParameter = like.getValueParameter();
    final Evaluation<?> patternParameter = like.getPatternParameter();
    final Evaluation<?> reconstructedValueParameter = this.reconstruct0(context, valueParameter);
    final Evaluation<?> reconstructedPatternParameter = this.reconstruct0(context, patternParameter);
    if (reconstructedValueParameter != valueParameter || reconstructedPatternParameter != patternParameter) {
      return new LikeExpression(
        (Evaluation<String>) reconstructedValueParameter,
        (Evaluation<String>) reconstructedPatternParameter
      );
    }
    return like;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final LogicalAnd logicalAnd, final TContext context) {
    final Evaluation<?> parameter1 = logicalAnd.getParameter1();
    final Evaluation<?> parameter2 = logicalAnd.getParameter2();
    final Evaluation<?> reconstructedParameter1 = this.reconstruct0(context, parameter1);
    final Evaluation<?> reconstructedParameter2 = this.reconstruct0(context, parameter2);
    if (reconstructedParameter1 != parameter1 || reconstructedParameter2 != parameter2) {
      return new LogicalAnd(
        (Evaluation<Boolean>) reconstructedParameter1,
        (Evaluation<Boolean>) reconstructedParameter2
      );
    }
    return logicalAnd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final LogicalXor logicalXor, final TContext context) {
    final Evaluation<?> parameter1 = logicalXor.getParameter1();
    final Evaluation<?> parameter2 = logicalXor.getParameter2();
    final Evaluation<?> reconstructedParameter1 = this.reconstruct0(context, parameter1);
    final Evaluation<?> reconstructedParameter2 = this.reconstruct0(context, parameter2);
    if (reconstructedParameter1 != parameter1 || reconstructedParameter2 != parameter2) {
      return new LogicalXor(
        (Evaluation<Boolean>) reconstructedParameter1,
        (Evaluation<Boolean>) reconstructedParameter2
      );
    }
    return logicalXor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final LogicalNot logicalNot, final TContext context) {
    final Evaluation<?> parameter = logicalNot.getParameter();
    final Evaluation<?> reconstructedParameter = this.reconstruct0(context, parameter);
    if (reconstructedParameter != parameter) {
      return new LogicalNot((Evaluation<Boolean>) reconstructedParameter);
    }
    return logicalNot;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Evaluation visit(final LogicalOr logicalOr, final TContext context) {
    final Evaluation<?> parameter1 = logicalOr.getParameter1();
    final Evaluation<?> parameter2 = logicalOr.getParameter2();
    final Evaluation<?> reconstructedParameter1 = this.reconstruct0(context, parameter1);
    final Evaluation<?> reconstructedParameter2 = this.reconstruct0(context, parameter2);
    if (reconstructedParameter1 != parameter1 || reconstructedParameter2 != parameter2) {
      return new LogicalAnd(
        (Evaluation<Boolean>) reconstructedParameter1,
        (Evaluation<Boolean>) reconstructedParameter2
      );
    }
    return logicalOr;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Expression visit(final Function<?> function, final TContext context) {
    final int functionParameterCount = function.getParameterCount();
    final Evaluation[] functionParameters = getFunctionParametersTemplate(functionParameterCount);
    try {
      for (int index = 0; index < functionParameterCount; index++) {
        functionParameters[index] = this.reconstruct0(context, function.getParameter(index));
      }
      for (int index = 0; index < functionParameterCount; index++) {
        if (function.getParameter(index) != functionParameters[index]) {
          final Function.Declare functionDeclare;
          try {
            functionDeclare = context.getFunctionManager().getFunctionDeclare(function.getName(), functionParameters);
          } catch (FunctionAmbiguousException exception) {
            throw new RuntimeException(exception);
          }
          return functionDeclare.newInstance(functionParameters);
        }
      }
      return Expression.class.cast(function);
    } finally {
      Arrays.fill(functionParameters, null);
    }
  }

  @Override
  public Expression<?> visit(final Case caze, final TContext context) {
    List<Case.Branch> reconstructedBranches = null;
    Evaluation<?> reconstructedOperateElse = null;

    // for condition and operation reconstruction
    List<Case.Branch> branches = caze.getBranches();
    for (int i = 0; i < branches.size(); i++) {
      Case.Branch<?> branch = branches.get(i);
      Evaluation<?> condition = branch.getCondition();
      Evaluation<?> operate = branch.getOperate();
      Evaluation<?> reconstructedCondition = (Evaluation<?>) reconstruct0(context, condition);
      Evaluation<?> reconstructedOperate = (Evaluation<?>) reconstruct0(context, operate);
      if (condition != reconstructedCondition || operate != reconstructedOperate) {
        if ( reconstructedBranches == null) {
          // do not forget the unchanged elements, so copy all and then set the changed element by index
          reconstructedBranches = new ArrayList<Case.Branch>(branches);
        }

        reconstructedBranches.set(i, new Case.Branch(reconstructedCondition, reconstructedOperate));
      }
    }

    // for else operation reconstruction
    if (caze.getOperateElse() != null) {
      reconstructedOperateElse = (Evaluation<?>) this.reconstruct0(context, caze.getOperateElse());
    }

    if (reconstructedBranches != null) {
      if (caze.getOperateElse() != reconstructedOperateElse) {
        return new Case(reconstructedBranches, reconstructedOperateElse);
      } else {
        return new Case(reconstructedBranches, caze.getOperateElse());
      }
    } else {
      if (caze.getOperateElse() != reconstructedOperateElse) {
        return new Case(caze.getBranches(), reconstructedOperateElse);
      } else {
        return caze;
      }
    }
  }

  private static Evaluation[] getFunctionParametersTemplate(final int parameterCount) {
    if (parameterCount > FUNCTION_PARAMETERS_TEMPLATE_CACHE_SIZE) {
      return new Evaluation[parameterCount];
    } else {
      final Map<Integer, Evaluation[]> cache = FUNCTION_PARAMETERS_TEMPLATE_CACHE.get();
      Evaluation[] template = cache.get(parameterCount);
      if (template == null) {
        template = new Evaluation[parameterCount];
        cache.put(parameterCount, template);
      }
      return template;
    }
  }

  private Expression reconstruct0(final TContext context, final Expression<?> expression) {
    if (expression == null) {
      return null;
    } else {
      return expression.accept(this, context);
    }
  }

  private Evaluation reconstruct0(final TContext context, final Evaluation<?> expression) {
    if (expression == null) {
      return null;
    } else {
      return (Evaluation) expression.accept(this, context);
    }
  }

  public static abstract class Context {

    public abstract FunctionManager getFunctionManager();

  }

}
