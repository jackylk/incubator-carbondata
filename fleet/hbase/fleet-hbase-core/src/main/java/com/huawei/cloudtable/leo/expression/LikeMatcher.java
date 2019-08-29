package com.huawei.cloudtable.leo.expression;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO 当前只支持，单个字符和多个字符匹配
public abstract class LikeMatcher {

  public static final char CHARACTER_1 = '_';

  public static final char CHARACTER_N = '%';

  public static final String ALL = Character.toString(CHARACTER_N);

  public static LikeMatcher of(final String pattern, final boolean caseSensitive) {
    if (pattern.length() == 1) {
      switch (pattern.charAt(0)) {
        case CHARACTER_1:
          return new LikeMatcher() {
            @Override
            public boolean matches(final String value) {
              return value.length() == 1;
            }
          };
        case CHARACTER_N:
          return new LikeMatcher() {
            @Override
            public boolean matches(final String value) {
              if (value == null) {
                throw new NullPointerException();
              }
              return true;
            }
          };
        default:
          if (caseSensitive) {
            return new LikeMatcher() {
              @Override
              public boolean matches(final String value) {
                return value.equals(pattern);
              }
            };
          } else {
            return new LikeMatcher() {
              @Override
              public boolean matches(final String value) {
                return value.equalsIgnoreCase(pattern);
              }
            };
          }
      }
    } else {
      // TODO 性能较差
      if (caseSensitive) {
        final Matcher matcher = Pattern.compile(toJavaPattern(pattern)).matcher("");
        return new LikeMatcher() {
          @Override
          public boolean matches(final String value) {
            matcher.reset(value);
            return matcher.matches();
          }
        };
      } else {
        final Matcher matcher = Pattern.compile(toJavaPattern(pattern).toUpperCase(Locale.ROOT)).matcher("");
        return new LikeMatcher() {
          @Override
          public boolean matches(final String value) {
            matcher.reset(value.toUpperCase(Locale.ROOT));
            return matcher.matches();
          }
        };
      }
    }
  }

  public static String getPrefix(final String pattern) {
    int index = 0;
    for (; index < pattern.length(); index++) {
      switch (pattern.charAt(index)) {
        case CHARACTER_1:
        case CHARACTER_N:
          break;
        default:
          continue;
      }
      break;
    }
    return index == 0 ? "" : index == pattern.length() ? pattern : pattern.substring(0, index);
  }

  public static boolean hasWildcard(final String pattern) {
    for (int index = 0; index < pattern.length(); index++) {
      switch (pattern.charAt(index)) {
        case CHARACTER_1:
        case CHARACTER_N:
          return true;
      }
    }
    return false;
  }

  public static String toJavaPattern(final String pattern) {
    final StringBuilder stringBuilder = new StringBuilder(pattern.length());
    // From the JDK doc: \Q and \E protect everything between them
    stringBuilder.append("\\Q");
    boolean wasSlash = false;
    for (int i = 0; i < pattern.length(); i++) {
      final char character = pattern.charAt(i);
      if (wasSlash) {
        stringBuilder.append(character);
        wasSlash = false;
      } else {
        switch (character) {
          case CHARACTER_1:
            stringBuilder.append("\\E.*\\Q");
            break;
          case CHARACTER_N:
            stringBuilder.append("\\E.\\Q");
            break;
          case '\\':
            wasSlash = true;
            break;
          default:
            stringBuilder.append(character);
        }
      }
    }
    stringBuilder.append("\\E");
    // Found nothing interesting
    return stringBuilder.toString();
  }

  private LikeMatcher() {
    // to do nothing.
  }

  public abstract boolean matches(String value);

}
