package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.expression.*;

public interface SQLExpressionVisitor<TResult, TParameter> {

  TResult visit(SQLAdditionExpression additionExpression, TParameter parameter);

  TResult visit(SQLAndExpression andExpression, TParameter parameter);

  TResult visit(SQLBetweenExpression betweenExpression, TParameter parameter);

  TResult visit(SQLConstant constant, TParameter parameter);

  TResult visit(SQLDivisionExpression divisionExpression, TParameter parameter);

  TResult visit(SQLEqualsExpression equalsExpression, TParameter parameter);

  TResult visit(SQLLikeExpression likeExpression, TParameter visitorParameter);

  TResult visit(SQLModuloExpression moduloExpression, TParameter parameter);

  TResult visit(SQLMultiplicationExpression multiplicationExpression, TParameter parameter);

  TResult visit(SQLNotEqualsExpression notEqualsExpression, TParameter parameter);

  TResult visit(SQLNotExpression notExpression, TParameter parameter);

  TResult visit(SQLFunctionExpression functionInvocation, TParameter parameter);

  TResult visit(SQLGreaterExpression greaterExpression, TParameter parameter);

  TResult visit(SQLGreaterOrEqualsExpression greaterOrEqualsExpression, TParameter parameter);

  TResult visit(SQLInExpression inExpression, TParameter parameter);

  TResult visit(SQLCaseExpression caseExpression, TParameter parameter);

  TResult visit(SQLIsNullExpression isNullExpression, TParameter parameter);

  TResult visit(SQLLessExpression lessExpression, TParameter parameter);

  TResult visit(SQLLessOrEqualsExpression lessOrEqualsExpression, TParameter parameter);

  TResult visit(SQLNull none, TParameter parameter);

  TResult visit(SQLOrExpression orExpression, TParameter parameter);

  TResult visit(SQLPreferentialExpression preferentialExpression, TParameter parameter);

  TResult visit(SQLReference.Column column, TParameter parameter);

  TResult visit(SQLReference.ColumnAll columnAll, TParameter parameter);

  TResult visit(SQLSubtractionExpression subtractionExpression, TParameter parameter);

  TResult visit(SQLVariable variable, TParameter parameter);

  TResult visit(SQLXorExpression exclusiveOrExpression, TParameter parameter);

}
