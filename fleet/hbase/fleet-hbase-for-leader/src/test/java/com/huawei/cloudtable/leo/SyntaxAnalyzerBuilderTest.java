package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.language.SQLStatement;
import com.huawei.cloudtable.leo.language.SyntaxAnalyzer;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLProject;

import org.junit.Assert;
import org.junit.Test;

public class SyntaxAnalyzerBuilderTest {

  @Test
  public void testInsert1() {
    testSuccess0("insert into test values " +
        "('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 30, 28, '2019-07-01 14:01:23'), " +
        "('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 30, 28, '2019-07-01 14:01:26')", SQLStatement.class);
  }

  @Test
  public void testSelect1() {
    testSuccess0("select address from employee where name='Zhang San'", SQLStatement.class);
  }

  @Test
  public void testSelect2() {
    testSuccess0("select age + 1 from employee where name='Zhang San'", SQLStatement.class);
  }

  @Test
  public void testSelect3() {
    testSuccess0("select age + 1, address from employee where name='Zhang San'", SQLStatement.class);
  }

  @Test
  public void testSelect4() {
    testSuccess0("select * from person where gender='Male' and married='True' and occupation='Teacher'", SQLStatement.class);
  }

  @Test
  public void testSelect5() {
    testSuccess0("select case when age < 18 then 'Y' when age < 60 then 'M' else 'O' end from person where gender='Male' and married='True' and occupation='Teacher'", SQLStatement.class);
  }

  @Test
  public void testProject() {
    testSuccess0("address", SQLProject.class);
  }

  @Test
  public void testExpression1() {
    testSuccess0("address", SQLExpression.class);
  }

  @Test
  public void testExpression2() {
    testSuccess0("age + 1", SQLExpression.class);
  }

  @Test
  public void testExpression3() {
    testSuccess0("name = 'Zhang San' and age + 2 * 3 = 10", SQLExpression.class);
  }

  private static <TNode extends SyntaxTree.Node> void testSuccess0(final String sql, final Class<TNode> expectedNodeClass) {
    final SyntaxAnalyzer syntaxAnalyzer = getTestSyntaxAnalyzer();
    final TNode node;
    try {
      node = syntaxAnalyzer.analyse(sql, expectedNodeClass).getRoot();
    } catch (SyntaxException e) {
      Assert.fail(e.getMessage());
      return;
    }
    Assert.assertNotNull(node);
  }

  private static SyntaxAnalyzer testSyntaxAnalyzer;

  private static SyntaxAnalyzer getTestSyntaxAnalyzer() {
    if (testSyntaxAnalyzer == null) {
      testSyntaxAnalyzer = SyntaxAnalyzerBuilder.build();
    }
    return testSyntaxAnalyzer;
  }

}
