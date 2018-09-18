package org.apache.carbondata.examples.sdk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;

public class CsvParserTest {

  static double apacheParserTime;
  static double univocityParserTime;
  static String data = "1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|";

  public static void main(String[] args) throws IOException {
    System.out.println("parsing file data:");
    testParseFile();
    System.out.println("\nparsing memory data:");
    testParseMem(600 * 10000);
  }

  private static void testParseMem(int round) {
    double start, end;

    start = System.nanoTime();
    for (int i = 0; i < round; i++) {
      String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, "|", 100);
    }
    end = System.nanoTime();
    System.out.println(String.format("apache common parser time: %.3f s, %,d lines", (end - start) / 1024 / 1024 / 1024, round));

    CsvParser parser = newUnivocityParser();
    start = System.nanoTime();
    for (int i = 0; i < round; i++) {
      String[] fields = parser.parseLine(data);
    }
    end = System.nanoTime();
    System.out.println(String.format("univocity parser time: %.3f s, %,d lines", (end - start) / 1024 / 1024 / 1024, round));
  }

  private static void testParseFile() throws IOException {
    int line = 0;
    double start;
    double end;

    start = System.nanoTime();
    for (int i = 0; i < 3; i++) {
      line = testApache("/Users/jacky/code/tpch-osx/dbgen/1g/lineitem2.tbl");
    }
    end = System.nanoTime();
    System.out.println(String.format("apache common total time: %.3f s, parser time: %.3f s, %,d lines",
        (end - start)/3 / 1024 / 1024 / 1024, apacheParserTime/3 / 1024 / 1024 / 1024, line));

    start = System.nanoTime();
    for (int i = 0; i < 3; i++) {
      line = testUnivocity("/Users/jacky/code/tpch-osx/dbgen/1g/lineitem.tbl");
    }
    end = System.nanoTime();
    System.out.println(String.format("univocity total time: %.3f s, parser time: %.3f s, %,d lines",
        (end - start)/3 / 1024 / 1024 / 1024, univocityParserTime/3 / 1024 / 1024 / 1024, line));
  }

  private static int testApache(String file) throws IOException {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new BOMInputStream(new FileInputStream(new File(file)))));
    String line = reader.readLine();
    int lineCount = 0;
    while (line != null) {
      long start = System.nanoTime();
      String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "|", 100);
      apacheParserTime += System.nanoTime() - start;
      lineCount++;
      line = reader.readLine();
    }
    reader.close();
    return lineCount;
  }

  private static int testApacheWithBuffer(String file) throws IOException {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new BOMInputStream(new FileInputStream(new File(file)))));
    String line = reader.readLine();
    int lineCount = 0;
    while (line != null) {
      long start = System.nanoTime();
      String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "|", 100);
      apacheParserTime += System.nanoTime() - start;
      lineCount++;
      line = reader.readLine();
    }
    reader.close();
    return lineCount;
  }

  private static int testUnivocity(String file) throws IOException {
    CsvParser parser = newUnivocityParser();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new BOMInputStream(new FileInputStream(new File(file)))));
    parser.beginParsing(reader);
    int lineCount = 0;
    String[] fields = parser.parseNext();
    while (fields != null) {
      long start = System.nanoTime();
      fields = parser.parseNext();
      univocityParserTime += System.nanoTime() - start;
      lineCount++;
    }
    reader.close();
    return lineCount;
  }

  private static CsvParser newUnivocityParser() {
    CsvParserSettings parserSettings = new CsvParserSettings();
    parserSettings.getFormat().setDelimiter('|');
    parserSettings.setMaxColumns(100);
    //    parserSettings.setLineSeparatorDetectionEnabled(true);
    //    parserSettings.setIgnoreLeadingWhitespaces(false);
    //    parserSettings.setIgnoreTrailingWhitespaces(false);
    //    parserSettings.setSkipEmptyLines(false);

    return new CsvParser(parserSettings);
  }
}
