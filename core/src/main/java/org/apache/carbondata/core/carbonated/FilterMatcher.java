package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.scan.expression.Expression;

public class FilterMatcher {
  public static boolean isHit(SplitScanner.RawColumnChunk filterPage,
      Expression filterExpression) {
    return false;
  }
}
