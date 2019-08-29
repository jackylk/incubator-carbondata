package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.common.CharacterHelper;

import java.util.Locale;

/**
 * ASCII characters, and case insensitive.
 */
public class Identifier {

  public static Identifier of(final String string) {
    return new Identifier(string);
  }

  public Identifier(final String string) {
    if (string == null) {
      throw new IllegalArgumentException("Argument [string] is null.");
    }
    final Integer firstIllegalCharacterIndex = getFirstIllegalCharacterIndex(string);
    if (firstIllegalCharacterIndex != null) {
      throw new IllegalArgumentException("Argument [string] contains illegal character. Character index is " + firstIllegalCharacterIndex + ".");
    }
    this.string = string.toLowerCase(Locale.ENGLISH);
  }

  protected final String string;

  public final int length() {
    return this.string.length();
  }

  @Override
  public final int hashCode() {
    return this.string.hashCode();
  }

  @Override
  public final boolean equals(final Object object) {
    if (object == this) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (object instanceof Identifier) {
      final Identifier that = (Identifier) object;
      return this.string.equals(that.string);
    } else {
      return false;
    }
  }

  @Override
  public final String toString() {
    return this.string;
  }

  public final boolean matches(final String pattern) {
    return this.string.matches(pattern);
  }

  private static Integer getFirstIllegalCharacterIndex(final String string) {
    for (int characterIndex = 0; characterIndex < string.length(); characterIndex++) {
      if (!CharacterHelper.isASCII(string.charAt(characterIndex))) {
        return characterIndex;
      }
    }
    return null;
  }

}
