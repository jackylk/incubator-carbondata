package com.huawei.cloudtable.leo.expressions;

import com.huawei.cloudtable.leo.ValueBytes;
import com.huawei.cloudtable.leo.ValueCodec;
import com.huawei.cloudtable.leo.expression.*;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.math.BigInteger;

public class InArrayExpressionTest {

  /**
   * for test method defination (ForInteger1、ForInteger2、ForString etc)
   */
  @Test
  public void testDeclare() {
    Function.getName(InArrayExpression.ForInteger1.class);
    for (Constructor constructor : InArrayExpression.ForInteger1.class.getConstructors()) {
      Function.getDeclare(constructor);
    }

    for (Constructor constructor : InArrayExpression.ForInteger2.class.getConstructors()) {
      Function.getDeclare(constructor);
    }

    for (Constructor constructor : InArrayExpression.ForInteger4.class.getConstructors()) {
      Function.getDeclare(constructor);
    }

    for (Constructor constructor : InArrayExpression.ForInteger8.class.getConstructors()) {
      Function.getDeclare(constructor);
    }

    for (Constructor constructor : InArrayExpression.ForInteger.class.getConstructors()) {
      Function.getDeclare(constructor);
    }

    for (Constructor constructor : InArrayExpression.ForString.class.getConstructors()) {
      Function.getDeclare(constructor);
    }
  }

  /**
   * for test method logic
   */
  @Test
  public void testEvaluteForInteger1() {
    final Reference<Byte> valueParameter = new Reference<>(0, Byte.class, false);
    final Evaluation<Byte>[] arrayParameter = new Evaluation[]{
      new Constant<Byte>(Byte.class, (byte) 1),
      new Constant<Byte>(Byte.class, (byte) 2),
      new Constant<Byte>(Byte.class, (byte) 4)
    };
    final InArrayExpression.ForInteger1 function = new InArrayExpression.ForInteger1(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{(byte) 5};
      Assert.assertEquals(false, function.evaluate(runtimeContext));


      runtimeContext.row = new Object[]{(byte) 1};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testEvaluteForInteger2() {
    final Reference<Short> valueParameter = new Reference<>(0, Short.class, false);
    final Evaluation<Short>[] arrayParameter = new Evaluation[]{
      new Constant<Short>(Short.class, (short) 1),
      new Constant<Short>(Short.class, (short) 2),
      new Constant<Short>(Short.class, (short) 4)
    };
    final InArrayExpression.ForInteger2 function = new InArrayExpression.ForInteger2(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{(short) 5};
      Assert.assertEquals(false, function.evaluate(runtimeContext));


      runtimeContext.row = new Object[]{(short) 1};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testEvaluteForInteger4() {
    final Reference<Integer> valueParameter = new Reference<>(0, Integer.class, false);
    final Evaluation<Integer>[] arrayParameter = new Evaluation[]{
      new Constant<Integer>(Integer.class, 1),
      new Constant<Integer>(Integer.class, 2),
      new Constant<Integer>(Integer.class, 4)
    };
    final InArrayExpression.ForInteger4 function = new InArrayExpression.ForInteger4(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{5};
      Assert.assertEquals(false, function.evaluate(runtimeContext));

      runtimeContext.row = new Object[]{1};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testEvaluteForInteger8() {
    final Reference<Long> valueParameter = new Reference<>(0, Long.class, false);
    final Evaluation<Long>[] arrayParameter = new Evaluation[]{
      new Constant<Long>(Long.class, 1L),
      new Constant<Long>(Long.class, 2L),
      new Constant<Long>(Long.class, 4L)
    };
    final InArrayExpression.ForInteger8 function = new InArrayExpression.ForInteger8(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{5L};
      Assert.assertEquals(false, function.evaluate(runtimeContext));

      runtimeContext.row = new Object[]{1L};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testEvaluteForBigInteger() {
    final Reference<BigInteger> valueParameter = new Reference<>(0, BigInteger.class, false);
    final Evaluation<BigInteger>[] arrayParameter = new Evaluation[] {
      new Constant<BigInteger>(BigInteger.class, new BigInteger("1")),
      new Constant<BigInteger>(BigInteger.class, new BigInteger("2")),
      new Constant<BigInteger>(BigInteger.class, new BigInteger("4"))
    };
    final InArrayExpression.ForInteger function = new InArrayExpression.ForInteger(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{new BigInteger("5")};
      Assert.assertEquals(false, function.evaluate(runtimeContext));

      runtimeContext.row = new Object[]{new BigInteger("1")};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testEvaluteForString() {
    final Reference<String> valueParameter = new Reference<>(0, String.class, false);
    final Evaluation<String>[] arrayParameter = new Evaluation[] {
      new Constant<String>(String.class, new String("1")),
      new Constant<String>(String.class, new String("2")),
      new Constant<String>(String.class, new String("4"))
    };
    final InArrayExpression.ForString function = new InArrayExpression.ForString(valueParameter, arrayParameter);
    final TestRuntimeContext runtimeContext = new TestRuntimeContext();
    try {
      runtimeContext.row = new Object[]{new String("5")};
      Assert.assertEquals(false, function.evaluate(runtimeContext));

      runtimeContext.row = new Object[]{new String("1")};
      Assert.assertEquals(true, function.evaluate(runtimeContext));
    } catch (EvaluateException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static final class TestRuntimeContext implements Evaluation.RuntimeContext {

    Object[] row;

    @Override
    public <TValue> TValue getVariable(Variable<TValue> variable) {
      return null;
    }

    @Override
    public <TValue> ValueBytes getVariableAsBytes(Variable<TValue> variable, ValueCodec<TValue> valueCodec) {
      return null;
    }

    @Override
    public <TValue> TValue getAttribute(Reference<TValue> reference) {
      return reference.getResultClass().cast(this.row[reference.getAttributeIndex()]);
    }

    @Override
    public <TValue> ValueBytes getAttributeAsBytes(Reference<TValue> reference, ValueCodec<TValue> valueCodec) {
      return null;
    }
  }

}
