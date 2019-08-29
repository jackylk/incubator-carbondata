package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.expressions.NotGreaterThanExpression;
import org.junit.Assert;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ExpressionCodecTest {

  @Test
  public void testCaseExpressionCodec() {
    ExpressionCodec.EncodeContext encodeContext = new ExpressionCodec.EncodeContext() {
      @Override
      public ValueCodecManager getValueCodecManager() {
        return HBaseValueCodecManager.BUILD_IN;
      }
    };

    ExpressionCodec.DecodeContext decodeContext = new ExpressionCodec.DecodeContext() {
      @Override
      public ValueCodecManager getValueCodecManager() {
        return HBaseValueCodecManager.BUILD_IN;
      }

      @Override
      public FunctionManager getFunctionManager() {
        return FunctionManager.BUILD_IN;
      }
    };

    List<Case.Branch<?>> branches = new ArrayList<>(1);

    Evaluation<Boolean> condition = new NotGreaterThanExpression.ForInteger4(
      new Reference<>(0, Integer.class, false),
      new Constant<>(Integer.class, 10));
    Evaluation<?> operate = new Constant<>(String.class, "children");

    branches.add(new Case.Branch<>(condition, operate));
    Evaluation<?> operateElse = null;
    Expression<?> expressionOrigin = new Case(branches, operateElse);

    byte[] bytes = ExpressionCodec.encode(encodeContext, expressionOrigin);
    Case<?> expressionDecode = (Case<?>) ExpressionCodec.decode(decodeContext, ByteBuffer.wrap(bytes));

    Assert.assertEquals(expressionOrigin, expressionDecode);
  }
}
