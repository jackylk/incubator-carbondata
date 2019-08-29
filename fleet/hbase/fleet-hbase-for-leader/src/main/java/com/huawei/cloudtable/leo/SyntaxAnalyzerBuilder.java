package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SQLSymbols;
import com.huawei.cloudtable.leo.language.SyntaxAnalyzer;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.expression.*;
import com.huawei.cloudtable.leo.language.statement.*;
import com.huawei.cloudtable.leo.language.statements.SQLInsertStatement;
import com.huawei.cloudtable.leo.language.statements.SQLSelectStatement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class SyntaxAnalyzerBuilder {

  static SyntaxAnalyzer build() {
    final Set<Character> whitespaceSet = new HashSet<>();
    whitespaceSet.add(' ');
    whitespaceSet.add('\t');
    whitespaceSet.add('\r');
    whitespaceSet.add('\n');

    final List<Class<? extends SyntaxTree.Node>> nodeClassList = new ArrayList<>();

    nodeClassList.add(SQLConstant.StringWord.class);
    nodeClassList.add(SQLConstant.NumberWord.class);
    nodeClassList.add(SQLIdentifier.LiteralWithQuota1.class);
    nodeClassList.add(SQLIdentifier.LiteralWithQuota2.class);
    nodeClassList.add(SQLKeywords.ALL.class);
    nodeClassList.add(SQLKeywords.AND.class);
    nodeClassList.add(SQLKeywords.AS.class);
    nodeClassList.add(SQLKeywords.ASC.class);
    nodeClassList.add(SQLKeywords.BY.class);
    nodeClassList.add(SQLKeywords.CONSTRAINT.class);
    nodeClassList.add(SQLKeywords.COMMIT.class);
    nodeClassList.add(SQLKeywords.CREATE.class);
    nodeClassList.add(SQLKeywords.CUBE.class);
    nodeClassList.add(SQLKeywords.DESC.class);
    nodeClassList.add(SQLKeywords.DEFAULT.class);
    nodeClassList.add(SQLKeywords.DROP.class);
    nodeClassList.add(SQLKeywords.EXCEPT.class);
    nodeClassList.add(SQLKeywords.EXISTS.class);
    nodeClassList.add(SQLKeywords.FALSE.class);
    nodeClassList.add(SQLKeywords.FROM.class);
    nodeClassList.add(SQLKeywords.FULL.class);
    nodeClassList.add(SQLKeywords.GROUP.class);
    nodeClassList.add(SQLKeywords.HAVING.class);
    nodeClassList.add(SQLKeywords.IN.class);
    nodeClassList.add(SQLKeywords.INDEX.class);
    nodeClassList.add(SQLKeywords.INNER.class);
    nodeClassList.add(SQLKeywords.INSERT.class);
    nodeClassList.add(SQLKeywords.INTO.class);
    nodeClassList.add(SQLKeywords.IS.class);
    nodeClassList.add(SQLKeywords.JOIN.class);
    nodeClassList.add(SQLKeywords.KEY.class);
    nodeClassList.add(SQLKeywords.LEFT.class);
    nodeClassList.add(SQLKeywords.LIKE.class);
    nodeClassList.add(SQLKeywords.CASE.class);
    nodeClassList.add(SQLKeywords.WHEN.class);
    nodeClassList.add(SQLKeywords.THEN.class);
    nodeClassList.add(SQLKeywords.ELSE.class);
    nodeClassList.add(SQLKeywords.END.class);
    nodeClassList.add(SQLKeywords.LIMIT.class);
    nodeClassList.add(SQLKeywords.NOT.class);
    nodeClassList.add(SQLKeywords.NULL.class);
    nodeClassList.add(SQLKeywords.OFFSET.class);
    nodeClassList.add(SQLKeywords.ON.class);
    nodeClassList.add(SQLKeywords.OR.class);
    nodeClassList.add(SQLKeywords.ORDER.class);
    nodeClassList.add(SQLKeywords.OUTER.class);
    nodeClassList.add(SQLKeywords.PRIMARY.class);
    nodeClassList.add(SQLKeywords.RIGHT.class);
    nodeClassList.add(SQLKeywords.ROLLBACK.class);
    nodeClassList.add(SQLKeywords.ROLLUP.class);
    nodeClassList.add(SQLKeywords.SCHEMA.class);
    nodeClassList.add(SQLKeywords.SELECT.class);
    nodeClassList.add(SQLKeywords.TABLE.class);
    nodeClassList.add(SQLKeywords.TRUE.class);
    nodeClassList.add(SQLKeywords.UNION.class);
    nodeClassList.add(SQLKeywords.USE.class);
    nodeClassList.add(SQLKeywords.VALUES.class);
    nodeClassList.add(SQLKeywords.WHERE.class);
    nodeClassList.add(SQLKeywords.WITH.class);

    nodeClassList.add(SQLSymbols.CEDILLA.class);
    nodeClassList.add(SQLSymbols.COMMA.class);
    nodeClassList.add(SQLSymbols.POINT.class);
    nodeClassList.add(SQLSymbols.SOLIDUS.class);
    nodeClassList.add(SQLSymbols.L_PARENTHESIS.class);
    nodeClassList.add(SQLSymbols.R_PARENTHESIS.class);
    nodeClassList.add(SQLSymbols.L_ANGLE_BRACKET.class);
    nodeClassList.add(SQLSymbols.R_ANGLE_BRACKET.class);
    nodeClassList.add(SQLSymbols.EQUAL.class);
    nodeClassList.add(SQLSymbols.QUESTION.class);
    nodeClassList.add(SQLSymbols.STAR.class);
    nodeClassList.add(SQLSymbols.COLON.class);
    nodeClassList.add(SQLSymbols.PLUS.class);
    nodeClassList.add(SQLSymbols.SUBTRACTION.class);

    nodeClassList.add(SQLAdditionExpression.class);
    nodeClassList.add(SQLAndExpression.class);
    nodeClassList.add(SQLBetweenExpression.class);
    nodeClassList.add(SQLConstant.String.class);
    nodeClassList.add(SQLConstant.Integer.class);
    nodeClassList.add(SQLConstant.Real.class);
    nodeClassList.add(SQLConstant.Boolean.class);
    nodeClassList.add(SQLDivisionExpression.class);
    nodeClassList.add(SQLEqualsExpression.class);
    nodeClassList.add(SQLFunctionExpression.class);
    nodeClassList.add(SQLGreaterExpression.class);
    nodeClassList.add(SQLGreaterOrEqualsExpression.class);
    nodeClassList.add(SQLInExpression.class);
    nodeClassList.add(SQLIsNullExpression.class);
    nodeClassList.add(SQLLessExpression.class);
    nodeClassList.add(SQLLessOrEqualsExpression.class);
    nodeClassList.add(SQLLikeExpression.class);
    nodeClassList.add(SQLCaseExpression.class);
    nodeClassList.add(SQLCaseExpression.Branch.class);
    nodeClassList.add(SQLModuloExpression.class);
    nodeClassList.add(SQLMultiplicationExpression.class);
    nodeClassList.add(SQLNotEqualsExpression.class);
    nodeClassList.add(SQLNotExpression.class);
    nodeClassList.add(SQLNull.class);
    nodeClassList.add(SQLOrExpression.class);
    nodeClassList.add(SQLPreferentialExpression.class);
    nodeClassList.add(SQLReference.Column.class);
    nodeClassList.add(SQLReference.ColumnAll.class);
    nodeClassList.add(SQLSubtractionExpression.class);
    nodeClassList.add(SQLVariable.class);
    nodeClassList.add(SQLXorExpression.class);

    nodeClassList.add(SQLAlias.class);
    nodeClassList.add(SQLFrom.class);
    nodeClassList.add(SQLGroupBy.class);
    nodeClassList.add(SQLGroupType.ROLLUP.class);
    nodeClassList.add(SQLGroupType.CUBE.class);
    nodeClassList.add(SQLHaving.class);
    nodeClassList.add(SQLHint.class);
    nodeClassList.add(SQLHints.class);
    nodeClassList.add(SQLIdentifier.class);
    nodeClassList.add(SQLLimit.class);
    nodeClassList.add(SQLOffset.class);
    nodeClassList.add(SQLProject.class);
    nodeClassList.add(SQLQuery.Select.class);
    nodeClassList.add(SQLTableIdentifier.class);
    nodeClassList.add(SQLTableReference.Named.class);
    nodeClassList.add(SQLWhere.class);
    nodeClassList.add(SQLValues.class);

    nodeClassList.add(SQLInsertStatement.FromValues.class);
    nodeClassList.add(SQLSelectStatement.class);

    return new SyntaxAnalyzer.Builder(whitespaceSet, false, nodeClassList).build();
  }

  private SyntaxAnalyzerBuilder() {
    // to do nothing.
  }

}
