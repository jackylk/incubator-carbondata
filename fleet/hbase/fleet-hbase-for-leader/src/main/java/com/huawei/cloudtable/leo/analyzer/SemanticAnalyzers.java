package com.huawei.cloudtable.leo.analyzer;

import com.huawei.cloudtable.leo.common.ExtensionCollector;
import com.huawei.cloudtable.leo.language.SQLStatement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class SemanticAnalyzers {

  @SuppressWarnings("unchecked")
  public static <TSourceStatement extends SQLStatement> SemanticAnalyzer<TSourceStatement, ?> getSemanticAnalyzer(
      final Class<TSourceStatement> sourceStatementClass
  ) {
    return (SemanticAnalyzer<TSourceStatement, ?>) ANALYZER_MAP.get(sourceStatementClass);
  }

  private static final Map<Class<?>, SemanticAnalyzer<?, ?>> ANALYZER_MAP;

  static {
    final Map<Class<?>, SemanticAnalyzer<?, ?>> analyzerMap = new HashMap<>();
    final Iterator<Class<? extends SemanticAnalyzer>> analyzerClassIterator = ExtensionCollector.getExtensionClasses(SemanticAnalyzer.class);
    while (analyzerClassIterator.hasNext()) {
      final SemanticAnalyzer analyzer;
      try {
        analyzer = analyzerClassIterator.next().newInstance();
      } catch (InstantiationException | IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
      analyzerMap.put(analyzer.getSourceClass(), analyzer);
    }
    ANALYZER_MAP = analyzerMap;
  }

  private SemanticAnalyzers() {
    // to do nothing.
  }

}
