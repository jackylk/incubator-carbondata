package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SyntaxTree;

public abstract class SQLExpression extends SyntaxTree.Node {

  public static abstract class Priority0 extends Priority1 {

  }

  public static abstract class Priority1 extends Priority2 {

  }

  public static abstract class Priority2 extends Priority3 {

  }

  public static abstract class Priority3 extends Priority4 {

  }

  public static abstract class Priority4 extends Priority5 {

  }

  public static abstract class Priority5 extends Priority6 {

  }

  public static abstract class Priority6 extends Priority7 {

  }

  public static abstract class Priority7 extends SQLExpression {

  }

  private SQLExpression() {
    // to do nothing.
  }

  public abstract <TVisitorResult, TVisitorParameter> TVisitorResult accept(
      SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
      TVisitorParameter visitorParameter
  );

}
