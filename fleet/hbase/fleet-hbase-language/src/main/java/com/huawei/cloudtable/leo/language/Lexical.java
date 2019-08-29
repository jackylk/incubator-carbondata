package com.huawei.cloudtable.leo.language;

import javax.annotation.Nonnull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public abstract class Lexical<TValue> extends SyntaxTree.Node {

  Lexical() {
    // to do nothing.
  }

  // For lexical link list.
  int index = -1;

  Lexical next;

  Lexical prev;

  public abstract TValue getValue();

  int getIndex() {
    return this.index;
  }

  public static final class Whitespace extends Lexical<Character> {

    public static final Whitespace INSTANCE = new Whitespace();

    public static final Character VALUE = ' ';

    private Whitespace() {
      // to do nothing.
    }

    @Nonnull
    @Override
    public Character getValue() {
      return VALUE;
    }

    @Override
    int getIndex() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SyntaxTree.NodePosition getPosition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return Character.toString(VALUE);
    }

    @Override
    public void toString(StringBuilder stringBuilder) {
      stringBuilder.append(VALUE);
    }

  }

  public static abstract class Symbol extends Lexical<Character> {

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Declare {

      char value();

    }

    @Nonnull
    public static Declare getDeclare(final Class<? extends Symbol> clazz) {
      if (clazz == Symbol.class) {
        throw new IllegalArgumentException();
      }
      final Declare[] declares = clazz.getAnnotationsByType(Declare.class);
      switch (declares.length) {
        case 0:
          throw new RuntimeException("There is no declare annotation on class. " + clazz.getName());
        case 1:
          return declares[0];
        default:
          throw new RuntimeException("There are more than one declare annotations on class." + clazz.getName());
      }
    }

    public Symbol() {
      // to do nothing.
    }

    @Override
    public final Character getValue() {
      return getDeclare(this.getClass()).value();
    }

    @Override
    public final String toString() {
      return Character.toString(this.getValue());
    }

    public final void toString(final StringBuilder stringBuilder) {
      stringBuilder.append(this.getValue());
    }

    @Override
    public final int hashCode() {
      return this.getClass().hashCode();
    }

    @Override
    public final boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null) {
        return false;
      }
      return object.getClass() == Symbol.class;
    }

  }

  public static class Word extends Lexical<String> {

    Word(final String value) {
      if (value == null) {
        throw new IllegalArgumentException("Argument [value] is null.");
      }
      this.value = value;
    }

    private final String value;

    @Override
    public String getValue() {
      return this.value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      stringBuilder.append(this.value);
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null) {
        return false;
      }
      return object.getClass() == this.getClass() && this.value.equals(((Word) object).value);
    }

    public static abstract class WithQuota extends Word {

      @Target(ElementType.TYPE)
      @Retention(RetentionPolicy.RUNTIME)
      public @interface Declare {

        Class<? extends Symbol> quota();

        Class<? extends Symbol> escape();

      }

      @Nonnull
      public static Declare getDeclare(final Class<? extends WithQuota> clazz) {
        if (clazz == WithQuota.class) {
          throw new IllegalArgumentException();
        }
        final Declare[] declares = clazz.getAnnotationsByType(Declare.class);
        switch (declares.length) {
          case 0:
            throw new RuntimeException("There is no declare annotation on class. " + clazz.getName());
          case 1:
            return declares[0];
          default:
            throw new RuntimeException("There are more than one declare annotations on class." + clazz.getName());
        }
      }

      public WithQuota(final String value) {
        super(value);
      }

      public final char getQuota() {
        return Symbol.getDeclare(getDeclare(this.getClass()).quota()).value();
      }

      @Override
      public String toString() {
        final char quota = this.getQuota();
        return quota + this.getValue() + quota;
      }

      @Override
      public void toString(final StringBuilder stringBuilder) {
        final char quota = this.getQuota();
        stringBuilder.append(quota).append(this.getValue()).append(quota);
      }

    }

    public static abstract class WithStart extends Word {

      @Target(ElementType.TYPE)
      @Retention(RetentionPolicy.RUNTIME)
      public @interface Declare {

        char[] startCharacters();

        char[] legalCharacters();

      }

      @Nonnull
      public static Declare getDeclare(final Class<? extends WithStart> clazz) {
        if (clazz == WithStart.class) {
          throw new IllegalArgumentException();
        }
        final Declare[] declares = clazz.getAnnotationsByType(Declare.class);
        switch (declares.length) {
          case 0:
            throw new RuntimeException("There is no declare annotation on class. " + clazz.getName());
          case 1:
            return declares[0];
          default:
            throw new RuntimeException("There are more than one declare annotations on class." + clazz.getName());
        }
      }

      public WithStart(final String value) {
        super(value);
      }

    }

  }

  public static abstract class Keyword extends Lexical<String> {

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Declare {

      String value();

      boolean caseSensitive();

    }

    @Nonnull
    public static Declare getDeclare(final Class<? extends Keyword> clazz) {
      if (clazz == Keyword.class) {
        throw new IllegalArgumentException();
      }
      final Declare[] declares = clazz.getAnnotationsByType(Declare.class);
      switch (declares.length) {
        case 0:
          throw new RuntimeException("There is no declare annotation on class. " + clazz.getName());
        case 1:
          final Declare declare = declares[0];
          final String value = declare.value();
          // TODO check value format.
          if (value.isEmpty()) {
            // TODO
            throw new UnsupportedOperationException();
          }
          return declare;
        default:
          throw new RuntimeException("There are more than one declare annotations on class." + clazz.getName());
      }
    }

    public Keyword() {
      // to do nothing.
    }

    @Override
    public final String getValue() {
      return getDeclare(this.getClass()).value();
    }

    @Override
    public final String toString() {
      return this.getValue();
    }

    public final void toString(final StringBuilder stringBuilder) {
      stringBuilder.append(this.getValue());
    }

    @Override
    public final int hashCode() {
      return this.getClass().hashCode();
    }

    @Override
    public final boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null) {
        return false;
      }
      return object.getClass() == this.getClass();
    }

  }

}
