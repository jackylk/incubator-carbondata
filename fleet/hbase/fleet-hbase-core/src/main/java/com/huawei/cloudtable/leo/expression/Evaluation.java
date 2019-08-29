package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.ValueType;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Arrays;

public abstract class Evaluation<TResult> extends Expression<TResult> {

  public interface CompileContext {

    Charset getCharset();

    <TValue> ValueType<TValue> getValueType(Class<TValue> valueClass);

    /**
     * The key is value class and charset.
     */
    <TValue> ValueCodec<TValue> getValueCodec(Class<TValue> valueClass);

    /**
     * Case insensitivity.
     */
    boolean hasHint(String hintName);

  }

  public static final class CompileHints {

    public static final String DISABLE_BYTE_COMPARE = "DISABLE_BYTE_COMPARE";

    private CompileHints() {
      // to do nothing.
    }

  }

  public interface RuntimeContext {

    <TValue> TValue getVariable(Variable<TValue> variable);

    <TValue> ValueBytes getVariableAsBytes(Variable<TValue> variable, ValueCodec<TValue> valueCodec);

    <TValue> TValue getAttribute(Reference<TValue> reference);

    <TValue> ValueBytes getAttributeAsBytes(Reference<TValue> reference, ValueCodec<TValue> valueCodec);

  }

  public static abstract class Evaluator<TResult> {

    public static final Evaluator<Boolean> ALWAYS_TRUE_EVALUATOR = new Evaluator<Boolean>() {
      @Override
      public Boolean evaluate(final Evaluation.RuntimeContext context) {
        return true;
      }
    };

    public static final Evaluator<Boolean> ALWAYS_FALSE_EVALUATOR = new Evaluator<Boolean>() {
      @Override
      public Boolean evaluate(final Evaluation.RuntimeContext context) {
        return false;
      }
    };

    public abstract TResult evaluate(Evaluation.RuntimeContext context) throws EvaluateException;

  }

  public static final Evaluation[] EMPTY_LIST = new Evaluation[0];

  public Evaluation(final Class<TResult> resultClass, final boolean nullable) {
    this(resultClass, nullable, EMPTY_LIST);
  }

  public Evaluation(final Class<TResult> resultClass, final boolean nullable, final Evaluation... parameterList) {
    super(resultClass, nullable);
    if (parameterList == null) {
      throw new IllegalArgumentException("Argument [parameterList] is null.");
    }
    this.parameterList = parameterList;
  }

  private final Evaluation[] parameterList;

  private volatile int hashCode = 0;

  public final int getParameterCount() {
    return this.parameterList.length;
  }

  /**
   * @throws IndexOutOfBoundsException If parameter index out build bounds.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  public final <TParameter> Evaluation<TParameter> getParameter(final int parameterIndex) {
    final Evaluation<?> evaluation = this.parameterList[parameterIndex];
    if (evaluation == null) {
      return null;
    } else {
      return (Evaluation<TParameter>) evaluation;
    }
  }

  /**
   * 子类在重装该方法时，必须要调用一次父类的实现。
   */
  public void compile(final CompileContext context) {
    for (int index = 0; index < this.parameterList.length; index++) {
      if (this.parameterList[index] == null) {
        continue;
      }
      this.parameterList[index].compile(context);
    }
  }

  public abstract TResult evaluate(RuntimeContext context) throws EvaluateException;

  @Override
  public int hashCode() {
    if (this.hashCode == 0) {
      int hashCode = 0;
      if (this instanceof Function) {
        hashCode = 31 * ((Function) this).getName().hashCode();
      }
      if (this instanceof Commutative) {
        hashCode += ((Commutative) this).getHashCode();
      } else {
        hashCode += Arrays.hashCode(this.parameterList);
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
      final Evaluation that = (Evaluation) object;
      if (that instanceof Function && !((Function) this).getName().equals(((Function) that).getName())) {
        return false;
      }
      if (that instanceof Commutative) {
        return ((Commutative) this).equals((Commutative) that);
      } else {
        return Arrays.equals(this.parameterList, that.parameterList);
      }
    } else {
      return false;
    }
  }

}
