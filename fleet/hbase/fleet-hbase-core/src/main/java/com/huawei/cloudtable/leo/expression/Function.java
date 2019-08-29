package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.common.CollectionHelper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.annotation.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface Function<TResult> {

  short NAME_MAXIMUM_LENGTH = Short.MAX_VALUE;

  static <TFunction extends Expression & Function<?>> Identifier getName(final Class<TFunction> functionClass) {
    final Name[] functionNames = functionClass.getAnnotationsByType(Name.class);
    if (functionNames.length == 0) {
      return null;
    }
    final String functionName = functionNames[0].value();
    if (functionName.length() > NAME_MAXIMUM_LENGTH) {
      throw new FunctionDeclareException(
          "Function name [" + functionName + "] is too long, the maximum getLength is " + NAME_MAXIMUM_LENGTH + "."
      );
    }
    return Identifier.of(functionName);
  }

  static <TValue, TFunction extends Expression<TValue> & Function<TValue>> Declare getDeclare(
      final Constructor<TFunction> functionConstructor
  ) {
    return DeclareHelper.getDeclare(functionConstructor);
  }

  final class Declare<TValue, TFunction extends Expression<TValue> & Function<TValue>> {

    Declare(
        final Identifier name,
        final ValueParameterDeclare[] valueParameterDeclares,
        final ArrayParameterDeclare arrayParameterDeclare,
        final Constructor<TFunction> constructor
    ) {
      final int parameterCount = valueParameterDeclares.length + (arrayParameterDeclare == null ? 0 : 1);
      final List<Class<?>> parameterClassList = new ArrayList<>(parameterCount);
      for (ValueParameterDeclare valueParameterDeclare : valueParameterDeclares) {
        parameterClassList.add(valueParameterDeclare.clazz);
      }
      if (arrayParameterDeclare != null) {
        parameterClassList.add(arrayParameterDeclare.clazz);
      }
      final Class<TFunction> declaringClass = constructor.getDeclaringClass();
      this.name = name;
      this.parameterClassList = Collections.unmodifiableList(parameterClassList);
      this.valueParameterDeclares = valueParameterDeclares;
      this.arrayParameterDeclare = arrayParameterDeclare;
      this.constructor = constructor;
      this.declaringClass = declaringClass;
      this.commutative = Commutative.class.isAssignableFrom(declaringClass);
    }

    private final Identifier name;

    private final List<Class<?>> parameterClassList;

    private final ValueParameterDeclare[] valueParameterDeclares;

    private final ArrayParameterDeclare arrayParameterDeclare;

    private final Constructor<TFunction> constructor;

    private final Class<TFunction> declaringClass;

    private final boolean commutative;

    @Nonnull
    public Class<TFunction> getDeclaringClass() {
      return this.declaringClass;
    }

    public Identifier getName() {
      return this.name;
    }

    @Nonnull
    public List<Class<?>> getParameterClassList() {
      return this.parameterClassList;
    }

    public int getValueParameterCount() {
      return this.valueParameterDeclares.length;
    }

    @Nonnull
    public ValueParameterDeclare getValueParameterDeclare(final int parameterIndex) {
      return this.valueParameterDeclares[parameterIndex];
    }

    @Nullable
    public ArrayParameterDeclare getArrayParameterDeclare() {
      return this.arrayParameterDeclare;
    }

    public boolean isCommutative() {
      return this.commutative;
    }

    public TFunction newInstance(final Evaluation<?>[] parameters) {
      // Check argument count.
      if (parameters.length < this.valueParameterDeclares.length) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (parameters.length > this.valueParameterDeclares.length && this.arrayParameterDeclare == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      // Check argument classes.
      int parameterIndex = 0;
      final int valueParameterCount = this.valueParameterDeclares.length;
      for (; parameterIndex < valueParameterCount; parameterIndex++) {
        if (parameters[parameterIndex] == null) {
          continue;
        }
        final Class<?> expectedParameterClass = this.valueParameterDeclares[parameterIndex].clazz;
        final Class<?> actualParameterClass = parameters[parameterIndex].getResultClass();
        if (!expectedParameterClass.isAssignableFrom(actualParameterClass)) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
      if (this.arrayParameterDeclare != null) {
        for (; parameterIndex < parameters.length; parameterIndex++) {
          if (parameters[parameterIndex] == null) {
            continue;
          }
          final Class<?> actualParameterClass = parameters[parameterIndex].getResultClass();
          if (!this.arrayParameterDeclare.elementClass.isAssignableFrom(actualParameterClass)) {
            // TODO
            throw new UnsupportedOperationException();
          }
        }
      }
      // Check nullable.
      for (; parameterIndex < valueParameterCount; parameterIndex++) {
        if (parameters[parameterIndex] == null && this.valueParameterDeclares[parameterIndex].required) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
      if (this.arrayParameterDeclare != null && !this.arrayParameterDeclare.elementNullable) {
        for (; parameterIndex < parameters.length; parameterIndex++) {
          if (parameters[parameterIndex] == null) {
            // TODO
            throw new UnsupportedOperationException();
          }
        }
      }
      // Construct.
      try {
        final Object[] constructorParameters;
        if (this.arrayParameterDeclare == null) {
          constructorParameters = parameters;
        } else {
          final Evaluation<?>[] arrayParameter;
          if (parameters.length == this.valueParameterDeclares.length) {
            arrayParameter = Evaluation.EMPTY_LIST;
          } else {
            arrayParameter = Arrays.copyOfRange(parameters, valueParameterCount, parameters.length);
          }
          constructorParameters = new Object[valueParameterCount + 1];
          System.arraycopy(parameters, 0, constructorParameters, 0, valueParameterCount);
          CollectionHelper.setLast(constructorParameters, arrayParameter);
        }
        return this.constructor.newInstance(constructorParameters);
      } catch (InstantiationException | IllegalAccessException exception) {
        throw new RuntimeException(exception);
      } catch (InvocationTargetException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }

    @Override
    public int hashCode() {
      int hashCode = this.name.hashCode();
      for (ValueParameterDeclare valueParameterDeclare : this.valueParameterDeclares) {
        hashCode = 31 * hashCode + valueParameterDeclare.hashCode();
      }
      if (this.arrayParameterDeclare != null) {
        hashCode = 31 * hashCode + this.arrayParameterDeclare.hashCode();
      }
      return hashCode;
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
        final Declare that = (Declare) object;
        return this.name.equals(that.name)
            && Arrays.equals(this.valueParameterDeclares, that.valueParameterDeclares)
            && (this.arrayParameterDeclare == null ? that.arrayParameterDeclare == null : this.arrayParameterDeclare.equals(that.arrayParameterDeclare));
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(this.name.toString());
      stringBuilder.append("(");
      int parameterIndex = 0;
      for (; parameterIndex < this.valueParameterDeclares.length; parameterIndex++) {
        if (parameterIndex != 0) {
          stringBuilder.append(", ");
        }
        stringBuilder.append(this.valueParameterDeclares[parameterIndex].getClazz().getName());
      }
      if (this.arrayParameterDeclare != null) {
        if (parameterIndex != 0) {
          stringBuilder.append(", ");
        }
        stringBuilder.append(this.arrayParameterDeclare.getElementClass().getName()).append("[]");
      }
      stringBuilder.append(")");
      return stringBuilder.toString();
    }

  }

  final class DeclareHelper {

    static <TValue, TFunction extends Expression<TValue> & Function<TValue>> Declare getDeclare(final Constructor<TFunction> functionConstructor) {
      final Class<TFunction> functionClass = functionConstructor.getDeclaringClass();
      if (functionClass.isArray()) {
        throw new IllegalArgumentException("The function class [" + functionClass.getName() + "] is an array.");
      }
      if (functionClass.isInterface()) {
        throw new IllegalArgumentException("The function class [" + functionClass.getName() + "] is an interface.");
      }
      if (Modifier.isAbstract(functionClass.getModifiers())) {
        throw new IllegalArgumentException("The function class [" + functionClass.getName() + "] is an abstract class.");
      }

      final Identifier functionName = Function.getName(functionClass);
      if (functionName == null) {
        throw new FunctionDeclareException("The function class [" + functionClass.getName() + "] without name declare.");
      }
      if (!Modifier.isPublic(functionConstructor.getModifiers())) {
        throw new FunctionDeclareException("The function constructor [" + functionConstructor + "] is not public.");
      }
      final ValueParameterDeclare[] valueParameterDeclares;
      final ArrayParameterDeclare arrayParameterDeclare;
      final int parameterCount = functionConstructor.getParameterCount();
      if (CollectionHelper.getLast(functionConstructor.getParameterTypes()).isArray()) {
        final int valueParameterCount = parameterCount - 1;
        valueParameterDeclares = new ValueParameterDeclare[valueParameterCount];
        for (int valueParameterIndex = 0; valueParameterIndex < valueParameterCount; valueParameterIndex++) {
          valueParameterDeclares[valueParameterIndex] = getValueParameterDeclare(functionConstructor, valueParameterIndex);
        }
        arrayParameterDeclare = getArrayParameterDeclare(functionConstructor, parameterCount - 1);
      } else {
        valueParameterDeclares = new ValueParameterDeclare[parameterCount];
        for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++) {
          valueParameterDeclares[parameterIndex] = getValueParameterDeclare(functionConstructor, parameterIndex);
        }
        arrayParameterDeclare = null;
      }

      final Declare<TValue, TFunction> declare = new Declare<>(
          functionName,
          valueParameterDeclares,
          arrayParameterDeclare,
          functionConstructor
      );

      if (declare.isCommutative() && (declare.getValueParameterCount() != 2 || declare.getArrayParameterDeclare() != null)) {
        // TODO
        throw new UnsupportedOperationException();
      }

      return declare;
    }

    private static ValueParameterDeclare getValueParameterDeclare(final Constructor<?> functionConstructor, final int parameterIndex) {
      final Class<?> parameterType = functionConstructor.getParameterTypes()[parameterIndex];
      if (parameterType != Evaluation.class) {
        throw new FunctionDeclareException(
            "The function parameter class can only be [" + Evaluation.class.getName() + "]. " + functionConstructor.toString()
        );
      }
      final ValueParameter parameterAnnotation = tryGetParameterAnnotation(functionConstructor, parameterIndex, ValueParameter.class);
      if (parameterAnnotation == null) {
        throw new FunctionDeclareException(
            "The function parameter[" + parameterIndex + "] without declare. " + functionConstructor.toString()
        );
      }
      return new ValueParameterDeclare(parameterAnnotation.clazz(), parameterAnnotation.required());
    }

    private static ArrayParameterDeclare getArrayParameterDeclare(final Constructor<?> functionConstructor, final int parameterIndex) {
      final Class<?> parameterType = functionConstructor.getParameterTypes()[parameterIndex];
      if (parameterType != Evaluation[].class) {
        // TODO
        throw new UnsupportedOperationException();
      }
      final ArrayParameter parameterAnnotation = tryGetParameterAnnotation(functionConstructor, parameterIndex, ArrayParameter.class);
      if (parameterAnnotation == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return new ArrayParameterDeclare(parameterAnnotation.clazz(), parameterAnnotation.required(), parameterAnnotation.elementNullable());
    }

    private static <TAnnotation> TAnnotation tryGetParameterAnnotation(
        final Constructor<?> functionConstructor,
        final int parameterIndex,
        final Class<TAnnotation> annotationClass
    ) {
      final Annotation[] parameterAnnotations = functionConstructor.getParameterAnnotations()[parameterIndex];
      if (parameterAnnotations.length > 0) {
        for (Annotation parameterAnnotation : parameterAnnotations) {
          if (parameterAnnotation.annotationType() == annotationClass) {
            return annotationClass.cast(parameterAnnotation);
          }
        }
      }
      return null;
    }

    private DeclareHelper() {
      // to do nothing.
    }

  }

  abstract class ParameterDeclare {

    ParameterDeclare(final Class<?> clazz, final boolean required) {
      this.clazz = clazz;
      this.required = required;
    }

    protected final Class<?> clazz;

    public Class<?> getClazz() {
      return this.clazz;
    }

    final boolean required;

    public boolean isRequired() {
      return this.required;
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object object);

  }

  final class ValueParameterDeclare extends ParameterDeclare {

    ValueParameterDeclare(final Class<?> clazz, final boolean required) {
      super(clazz, required);
    }

    @Override
    public int hashCode() {
      return this.clazz.hashCode();
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
        final ValueParameterDeclare that = (ValueParameterDeclare) object;
        return this.clazz.equals(that.clazz) && this.required == that.required;
      } else {
        return false;
      }
    }

  }

  final class ArrayParameterDeclare extends ParameterDeclare {

    ArrayParameterDeclare(final Class<?> clazz, final boolean required, final boolean elementNullable) {
      super(clazz, required);
      if (!clazz.isArray()) {
        throw new IllegalArgumentException("Argument [clazz] is not an array class.");
      }
      this.elementClass = clazz.getComponentType();
      this.elementNullable = elementNullable;
    }

    private final Class<?> elementClass;

    private final boolean elementNullable;

    public Class<?> getElementClass() {
      return this.elementClass;
    }

    public boolean isElementNullable() {
      return this.elementNullable;
    }

    @Override
    public int hashCode() {
      return this.elementClass.hashCode();
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
        final ArrayParameterDeclare that = (ArrayParameterDeclare) object;
        return this.elementClass.equals(that.elementClass)
            && this.required == that.required
            && this.elementNullable == that.elementNullable;
      } else {
        return false;
      }
    }

  }

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Name {

    String value();

  }

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @interface ValueParameter {

    Class clazz();

    boolean required() default true;

  }

  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  @interface ArrayParameter {

    Class clazz();

    boolean required() default true;

    boolean elementNullable() default false;

  }

  @Nonnull
  Identifier getName();

  @Nonnull
  Class<TResult> getResultClass();

  int getParameterCount();

  /**
   * @throws IndexOutOfBoundsException If parameter index out build bounds.
   */
  <TParameter> Evaluation<TParameter> getParameter(int parameterIndex);

  void toString(StringBuilder stringBuilder);

  static void toString(final Function function, final StringBuilder stringBuilder) {
    stringBuilder.append(function.getName().toString());
    stringBuilder.append("(");
    for (int parameterIndex = 0; parameterIndex < function.getParameterCount(); parameterIndex++) {
      if (parameterIndex != 0) {
        stringBuilder.append(", ");
      }
      if (function.getParameter(parameterIndex) == null) {
        stringBuilder.append("null");
      } else {
        function.getParameter(parameterIndex).toString(stringBuilder);
      }
    }
    stringBuilder.append(")");
  }

}
