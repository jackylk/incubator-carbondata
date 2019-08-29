package com.huawei.cloudtable.leo.language;

import com.huawei.cloudtable.leo.language.json.JSONConstants;
import com.huawei.cloudtable.leo.language.json.JSONKeywords;
import com.huawei.cloudtable.leo.language.json.JSONSymbols;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LexicalAnalyzerTest {

  @Test
  public void test() {
    final LexicalAnalyzer lexicalAnalyzer = getTestLexicalAnalyzer();
    final LexicalIterator lexicalIterator = lexicalAnalyzer.analyse("{\n" +
        "\t\"name\": \"Zhang San\",\n" +
        "\t\"age\": 18,\n" +
        "\t\"address\": {\n" +
        "\t\t\"country\": \"China\",\n" +
        "\t\t\"city\": \"Shen Zhen\"\n" +
        "\t}\n" +
        "}");
    Lexical lexical;
    while ((lexical = lexicalIterator.next()) != null) {
      System.out.println(lexical.getPosition() + "\t" + lexical);
    }
  }

  @Test
  public void testLoop() {
    final LexicalAnalyzer lexicalAnalyzer = getTestLexicalAnalyzer();
    final long startTime = System.nanoTime();
    for (int i = 0; i < 1000000; i++) {
      final LexicalIterator lexicalIterator = lexicalAnalyzer.analyse("{\n" +
          "\t\"name\": \"Zhang San\",\n" +
          "\t\"age\": 18,\n" +
          "\t\"address\": {\n" +
          "\t\t\"country\": \"China\",\n" +
          "\t\t\"city\": \"Shen Zhen\"\n" +
          "\t}\n" +
          "}");
      while (lexicalIterator.next() != null) {
        // to do nothing.
      }
    }
    System.out.println(TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) + "ms.");
  }

  private static LexicalAnalyzer getTestLexicalAnalyzer() {
    final Set<Character> whitespaceSet = new HashSet<>();
    whitespaceSet.add(' ');
    whitespaceSet.add('\t');
    whitespaceSet.add('\r');
    whitespaceSet.add('\n');

    final List<Class<? extends Lexical>> nodeClassList = new ArrayList<>();

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

    return new LexicalAnalyzer.Builder(whitespaceSet, false, nodeClassList).build();
  }

}
