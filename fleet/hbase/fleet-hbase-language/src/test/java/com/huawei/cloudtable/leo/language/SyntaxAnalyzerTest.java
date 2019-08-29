package com.huawei.cloudtable.leo.language;

import com.huawei.cloudtable.leo.language.json.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SyntaxAnalyzerTest {

  @Test
  public void test() {
    final List<String> jsonList = new ArrayList<>();
    jsonList.add(
        "{}"
    );
    jsonList.add(
        "[]"
    );
    jsonList.add(
        "\"\""
    );
    jsonList.add(
        "\"test\""
    );
    jsonList.add(
        "\".\""
    );
    jsonList.add(
        "123"
    );
    jsonList.add(
        ".123"
    );
    jsonList.add(
        "0.123"
    );
    jsonList.add(
        "123.123"
    );
    jsonList.add(
        "NULL"
    );
    jsonList.add(
        "TRUE"
    );
    jsonList.add(
        "FALSE"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": \"Zhang San\"\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": \":\"\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": NULL\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": TRUE\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": 123\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": .123\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": 123.123\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": []\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": \"Zhang San\",\n" +
            "\t\"age\": 18\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": \"Zhang San\",\n" +
            "\t\"age\": 18,\n" +
            "\t\"properties\": []\n" +
            "}"
    );
    jsonList.add(
        "{\n" +
            "\t\"name\": \"Zhang San\",\n" +
            "\t\"age\": 18,\n" +
            "\t\"address\": {\n" +
            "\t\t\"country\": \"China\",\n" +
            "\t\t\"city\": \"Shen Zhen\"\n" +
            "\t}\n" +
            "}"
    );
    for (String json : jsonList) {
      test0(json);
    }
  }

  private static void test0(final String json) {
    final JSONElement element;
    try {
      element = getTestSyntaxAnalyzer().analyse(json, JSONElement.class).getRoot();
    } catch (SyntaxException exception) {
      exception.printStackTrace();
      Assert.fail(json);
      return;
    }
    Assert.assertEquals(json, new JSONPrinter("\n", "\t").print(element));
  }

  private static SyntaxAnalyzer testSyntaxAnalyzer;

  private static SyntaxAnalyzer getTestSyntaxAnalyzer() {
    if (testSyntaxAnalyzer != null) {
      return testSyntaxAnalyzer;
    }

    final Set<Character> whitespaceSet = new HashSet<>();
    whitespaceSet.add(' ');
    whitespaceSet.add('\t');
    whitespaceSet.add('\r');
    whitespaceSet.add('\n');

    final List<Class<? extends SyntaxTree.Node>> nodeClassList = new ArrayList<>();

    nodeClassList.add(JSONConstants.StringWord.class);
    nodeClassList.add(JSONConstants.NumberWord.class);
    nodeClassList.add(JSONKeywords.TRUE.class);
    nodeClassList.add(JSONKeywords.FALSE.class);
    nodeClassList.add(JSONKeywords.NULL.class);

    nodeClassList.add(JSONSymbols.DOUBLE_QUOTA.class);
    nodeClassList.add(JSONSymbols.COMMA.class);
    nodeClassList.add(JSONSymbols.L_BRACE.class);
    nodeClassList.add(JSONSymbols.R_BRACE.class);
    nodeClassList.add(JSONSymbols.L_SQUARE_BRACKET.class);
    nodeClassList.add(JSONSymbols.R_SQUARE_BRACKET.class);
    nodeClassList.add(JSONSymbols.COLON.class);
    nodeClassList.add(JSONSymbols.POINT.class);

    nodeClassList.add(JSONNull.class);
    nodeClassList.add(JSONArray.class);
    nodeClassList.add(JSONObject.class);
    nodeClassList.add(JSONObjectField.class);
    nodeClassList.add(JSONConstants.String.class);
    nodeClassList.add(JSONConstants.Integer.class);
    nodeClassList.add(JSONConstants.Real.class);
    nodeClassList.add(JSONConstants.Boolean.class);

    testSyntaxAnalyzer = new SyntaxAnalyzer.Builder(whitespaceSet, false, nodeClassList).build();
    return testSyntaxAnalyzer;
  }

}
