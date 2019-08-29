package com.huawei.cloudtable.leo.language;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.*;

final class LexicalAnalyzer {

  private LexicalAnalyzer(
      final Set<Character> whitespaceSet,
      final boolean caseSensitive,
      final Map<Character, Constructor<? extends Lexical.Symbol>> symbolConstructorMap,
      final Map<String, Constructor<? extends Lexical.Keyword>> keywordConstructorMap,
      final Map<Character, Reader<?>> wordReaderMap
  ) {
    // 空格是必须的空白字符
    whitespaceSet.add(Lexical.Whitespace.VALUE);
    final Keywords keywords = new Keywords(caseSensitive);
    for (Map.Entry<String, Constructor<? extends Lexical.Keyword>> entry : keywordConstructorMap.entrySet()) {
      keywords.register(entry.getKey(), entry.getValue());
    }
    this.whitespaceSet = whitespaceSet;
    this.symbolConstructorMap = symbolConstructorMap;
    this.keywords = keywords;
    this.wordReaderMap = wordReaderMap;
  }

  private final Set<Character> whitespaceSet;

  private final Map<Character, Constructor<? extends Lexical.Symbol>> symbolConstructorMap;

  private final Keywords keywords;

  private final Map<Character, Reader<?>> wordReaderMap;

  public LexicalIterator analyse(final String string) {
    return new Context(this, string);
  }

  private boolean isWhitespaces(final char character) {
    return this.whitespaceSet.contains(character);
  }

  private boolean isWhitespaceOrSymbol(final char character) {
    return this.whitespaceSet.contains(character) || this.symbolConstructorMap.containsKey(character);
  }

  static final class Builder {

    @SuppressWarnings("unchecked")
    Builder(final Set<Character> whitespaceSet, final boolean caseSensitive, final List<Class<? extends Lexical>> lexicalClassList) {
      final Set<Class<? extends Lexical.Symbol>> symbolClassSet = new HashSet<>();
      final Set<Class<? extends Lexical.Keyword>> keywordClassSet = new HashSet<>();
      final Set<Class<? extends Lexical.Word>> wordClassSet = new HashSet<>();
      for (Class<? extends Lexical> lexicalClass : lexicalClassList) {
        if (!Modifier.isPublic(lexicalClass.getModifiers())) {
          // TODO 抛异常
          throw new UnsupportedOperationException();
        }
        if (Modifier.isAbstract(lexicalClass.getModifiers())) {
          // TODO 抛异常
          throw new UnsupportedOperationException();
        }
        if (Lexical.Symbol.class.isAssignableFrom(lexicalClass)) {
          symbolClassSet.add((Class<? extends Lexical.Symbol>) lexicalClass);
          continue;
        }
        if (Lexical.Keyword.class.isAssignableFrom(lexicalClass)) {
          keywordClassSet.add((Class<? extends Lexical.Keyword>) lexicalClass);
          continue;
        }
        if (Lexical.Word.class.isAssignableFrom(lexicalClass)) {
          wordClassSet.add((Class<? extends Lexical.Word>) lexicalClass);
          continue;
        }
        throw new UnsupportedOperationException();
      }
      final Set<Character> clonedWhitespaceSet = new HashSet<>(whitespaceSet.size() * 2);
      for (Character whitespace : whitespaceSet) {
        if (caseSensitive) {
          clonedWhitespaceSet.add(whitespace);
        } else {
          clonedWhitespaceSet.add(Character.toLowerCase(whitespace));
          clonedWhitespaceSet.add(Character.toUpperCase(whitespace));
        }
      }
      this.whitespaceSet = clonedWhitespaceSet;
      this.caseSensitive = caseSensitive;
      this.symbolConstructorMap = new HashMap<>();
      this.keywordConstructorMap = new HashMap<>();
      this.wordReaderMap = new HashMap<>();
      for (Class<? extends Lexical.Symbol> symbolClass : symbolClassSet) {
        this.registerSymbol(symbolClass, caseSensitive);
      }
      for (Class<? extends Lexical.Keyword> keywordClass : keywordClassSet) {
        this.registerKeyword(keywordClass);
      }
      for (Class<? extends Lexical.Word> nodeClass : wordClassSet) {
        this.registerWord(nodeClass);
      }
    }

    private final Set<Character> whitespaceSet;

    private final boolean caseSensitive;

    private final Map<Character, Constructor<? extends Lexical.Symbol>> symbolConstructorMap;

    private final Map<String, Constructor<? extends Lexical.Keyword>> keywordConstructorMap;

    private final Map<Character, Reader<?>> wordReaderMap;

    @SuppressWarnings("unchecked")
    private void registerSymbol(final Class<? extends Lexical.Symbol> symbolClass, final boolean caseSensitive) {
      final char symbolValue = Lexical.Symbol.getDeclare(symbolClass).value();
      if (this.symbolConstructorMap.containsKey(symbolValue)) {
        return;
      }
      final Constructor<? extends Lexical.Symbol> symbolConstructor;
      try {
        symbolConstructor = symbolClass.getConstructor();
      } catch (NoSuchMethodException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
      if (!Modifier.isPublic(symbolConstructor.getModifiers())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (caseSensitive) {
        this.symbolConstructorMap.put(symbolValue, symbolConstructor);
      } else {
        this.symbolConstructorMap.put(Character.toLowerCase(symbolValue), symbolConstructor);
        this.symbolConstructorMap.put(Character.toUpperCase(symbolValue), symbolConstructor);
      }
    }

    @SuppressWarnings("unchecked")
    private void registerKeyword(final Class<? extends Lexical.Keyword> keywordClass) {
      final String keywordValue = Lexical.Keyword.getDeclare(keywordClass).value();
      if (this.keywordConstructorMap.containsKey(keywordValue)) {
        return;
      }
      final Constructor<? extends Lexical.Keyword> keywordConstructor;
      try {
        keywordConstructor = keywordClass.getConstructor();
      } catch (NoSuchMethodException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
      if (!Modifier.isPublic(keywordConstructor.getModifiers())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      this.keywordConstructorMap.put(keywordValue, keywordConstructor);
    }

    @SuppressWarnings("unchecked")
    private <TWord extends Lexical.Word> void registerWord(Class<TWord> wordClass) {
      if (Lexical.Word.WithQuota.class.isAssignableFrom(wordClass)) {
        this.registerWordWithQuota((Class<? extends Lexical.Word.WithQuota>) wordClass);
      } else if (Lexical.Word.WithStart.class.isAssignableFrom(wordClass)) {
        this.registerWordWithStart((Class<? extends Lexical.Word.WithStart>) wordClass);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    private <TWord extends Lexical.Word.WithQuota> void registerWordWithQuota(Class<TWord> wordClass) {
      final Lexical.Word.WithQuota.Declare wordDeclare = Lexical.Word.WithQuota.getDeclare(wordClass);
      final char quota = Lexical.Symbol.getDeclare(wordDeclare.quota()).value();
      if (this.wordReaderMap.containsKey(quota)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      final Reader reader = new Reader.WithQuota<>(quota, wordClass);
      this.wordReaderMap.put(quota, reader);
    }

    private <TWord extends Lexical.Word.WithStart> void registerWordWithStart(Class<TWord> wordClass) {
      final Lexical.Word.WithStart.Declare wordDeclare = Lexical.Word.WithStart.getDeclare(wordClass);
      final Set<Character> legalCharacters = new HashSet<>();
      for (char legalCharacter : wordDeclare.legalCharacters()) {
        legalCharacters.add(legalCharacter);
      }
      for (char startCharacter : wordDeclare.startCharacters()) {
        if (this.wordReaderMap.containsKey(startCharacter)) {
          // TODO
          throw new UnsupportedOperationException();
        }
        final Reader.WithStart wordReader = new Reader.WithStart<>(legalCharacters, wordClass);
        this.wordReaderMap.put(startCharacter, wordReader);
      }
    }

    LexicalAnalyzer build() {
      return new LexicalAnalyzer(
          this.whitespaceSet,
          this.caseSensitive,
          this.symbolConstructorMap,
          this.keywordConstructorMap,
          this.wordReaderMap
      );
    }

  }

  private static abstract class Reader<TWord extends Lexical.Word> {

    Reader(final Class<TWord> wordClass) {
      try {
        this.wordConstructor = wordClass.getConstructor(String.class);
      } catch (NoSuchMethodException exception) {
        throw new RuntimeException(exception);
      }
    }

    private final Constructor<TWord> wordConstructor;

    TWord read(final Context context) {
      final int startPosition = context.characterCurrentIndex;
      final String string = this.readString(context);
      try {
        final TWord word = this.wordConstructor.newInstance(string);
        word.setPosition(SyntaxTree.NodePosition.of(startPosition, context.characterCurrentIndex));
        return word;
      } catch (Throwable exception) {
        throw new RuntimeException(exception);
      }
    }

    abstract String readString(final Context context);

    // TODO 字符串的场景
    private static final class WithQuota<TWord extends Lexical.Word.WithQuota> extends Reader<TWord> {

      WithQuota(final char quotaCharacter, final Class<TWord> wordClass) {
        super(wordClass);
        this.quotaCharacter = quotaCharacter;
      }

      private final char quotaCharacter;

      @Override
      String readString(final Context context) {
        final int startPosition = context.characterCurrentIndex;
        do {
          context.characterCurrentIndex++;
          if (context.characterCurrentIndex > context.characterMaximumIndex) {
            // TODO 抛异常，没有结束界符
            throw new UnsupportedOperationException();
          }
        } while (context.string.charAt(context.characterCurrentIndex) != this.quotaCharacter);
        return context.string.substring(startPosition + 1, context.characterCurrentIndex);
      }

    }

    // TODO 数字的场景
    private static final class WithStart<TWord extends Lexical.Word.WithStart> extends Reader<TWord> {

      WithStart(final Set<Character> legalCharacters, final Class<TWord> wordClass) {
        super(wordClass);
        this.legalCharacters = legalCharacters;
      }

      private final Set<Character> legalCharacters;

      @Override
      String readString(final Context context) {
        final int startPosition = context.characterCurrentIndex;
        while (true) {
          context.characterCurrentIndex++;
          if (context.characterCurrentIndex > context.characterMaximumIndex) {
            break;
          }
          final char character = context.string.charAt(context.characterCurrentIndex);
          if (context.analyser.isWhitespaceOrSymbol(character)) {
            break;
          }
          if (!this.legalCharacters.contains(character)) {
            // TODO 抛异常，语法错误
            throw new UnsupportedOperationException();
          }
        }
        final String string = context.string.substring(startPosition, context.characterCurrentIndex);
        context.characterCurrentIndex--;
        return string;
      }

    }

  }

  private static final class Context extends LexicalIterator {

    private static final Lexical THE_HEAD = new Lexical.Word("THE_HEAD");

    private static final Lexical THE_TAIL = new Lexical.Word("THE_TAIL");

    Context(final LexicalAnalyzer analyser, final String string) {
      this.analyser = analyser;
      this.string = string;
      this.characterMaximumIndex = string.length() - 1;
      this.reset();
    }

    private final LexicalAnalyzer analyser;

    private final String string;

    private final int characterMaximumIndex;

    private int characterCurrentIndex;

    private int theNextIndex;

    private Lexical theLast;

    private Lexical theCurrent;

    @Override
    String getString() {
      return this.string;
    }

    @Override
    void toHead() {
      this.reset();
    }

    @Override
    void toTail() {
      if (this.theCurrent == THE_TAIL) {
        return;
      }
      if (this.theCurrent == THE_HEAD) {
        while(true) {
          if (this.next() == null){
            break;
          }
        }
      } else {
        while(true) {
          if (this.next(this.theLast) == null){
            break;
          }
        }
      }
      this.theCurrent = THE_TAIL;
    }

    @Override
    Lexical prev(final Lexical lexical) {
      return lexical.prev;
    }

    @Override
    Lexical prev() {
      if (this.theCurrent == THE_TAIL) {
        if (this.theLast == null){
          this.reset();
        } else {
          this.theCurrent = this.theLast;
        }
        return this.theLast;
      }
      if (this.theCurrent == THE_HEAD) {
        return null;
      }
      final Lexical prev = this.theCurrent.prev;
      if (prev == null) {
        this.reset();
      }else {
        this.theCurrent = prev;
      }
      return prev;
    }

    @Override
    Lexical next(final Lexical lexical) {
      if (lexical.next != null) {
        return lexical.next;
      }
      if (lexical != this.theLast) {
        throw new RuntimeException();
      }
      final Lexical next = this.readNext();
      if (next != null) {
        next.prev = this.theLast;
        this.theLast.next = next;
        this.theLast = next;
      }
      return next;
    }

    @Override
    Lexical next() {
      if (this.theCurrent == THE_TAIL) {
        return null;
      }
      if (this.theCurrent != THE_HEAD && this.theCurrent != this.theLast) {
        final Lexical lexical = this.theCurrent.next;
        this.theCurrent = lexical;
        return lexical;
      }
      final Lexical lexical = this.readNext();
      if (lexical == null) {
        this.theCurrent = THE_TAIL;
        return null;
      }
      if (this.theLast != null) {
        lexical.prev = this.theLast;
        this.theLast.next = lexical;
      }
      this.theLast = lexical;
      this.theCurrent = lexical;
      return lexical;
    }

    private Lexical readNext() {
      final Lexical next = this.readNext0();
      if (next != null) {
        next.index = this.theNextIndex;
        this.theNextIndex++;
      }
      return next;
    }

    private Lexical readNext0() {
      while (true) {
        if (this.characterCurrentIndex > this.characterMaximumIndex) {
          return null;
        }
        if (!this.analyser.isWhitespaces(this.string.charAt(this.characterCurrentIndex))) {
          break;
        }
        this.characterCurrentIndex++;
      }
      final char character = this.string.charAt(this.characterCurrentIndex);
      // Word.
      final Reader<?> lexicalReader = this.analyser.wordReaderMap.get(character);
      if (lexicalReader != null) {
        final Lexical.Word word = lexicalReader.read(this);
        this.characterCurrentIndex++;
        return word;
      }
      // Symbol.
      final Constructor<? extends Lexical.Symbol> symbolConstructor = this.analyser.symbolConstructorMap.get(character);
      if (symbolConstructor != null) {
        final Lexical.Symbol symbol;
        try {
          symbol = symbolConstructor.newInstance();
        } catch (Throwable exception) {
          throw new RuntimeException(exception);
        }
        symbol.setPosition(SyntaxTree.NodePosition.of(this.characterCurrentIndex));
        this.characterCurrentIndex++;
        return symbol;
      }
      // Other.(Keyword or Word)
      final int startPosition = this.characterCurrentIndex;
      while (true) {
        this.characterCurrentIndex++;
        if (this.characterCurrentIndex > this.characterMaximumIndex) {
          break;
        }
        if (this.analyser.isWhitespaceOrSymbol(this.string.charAt(this.characterCurrentIndex))) {
          break;
        }
      }
      final SyntaxTree.NodePosition position = SyntaxTree.NodePosition.of(startPosition, this.characterCurrentIndex - 1);
      final Constructor<? extends Lexical.Keyword> keywordConstructor = this.analyser.keywords.getConstructor(this.string, startPosition, this.characterCurrentIndex);
      final Lexical lexical;
      if (keywordConstructor == null) {
        lexical = new Lexical.Word(this.string.substring(startPosition, this.characterCurrentIndex));
      } else {
        try {
          lexical = keywordConstructor.newInstance();
        } catch (Throwable exception) {
          throw new RuntimeException(exception);
        }
      }
      lexical.setPosition(position);
      return lexical;
    }

    private void reset() {
      this.characterCurrentIndex = 0;
      this.theNextIndex = 0;
      this.theLast = null;
      this.theCurrent = THE_HEAD;
    }

  }

  private static final class Keywords {

    Keywords(final boolean caseSensitive) {
      this.root = new Node();
      this.caseSensitive = caseSensitive;
    }

    private final Node root;

    private final boolean caseSensitive;

    final Constructor<? extends Lexical.Keyword> getConstructor(
        final String string,
        final int startCharacterIndex,
        final int endCharacterIndex
    ) {
      Node node = this.root;
      for (int index = startCharacterIndex; index < endCharacterIndex; index++) {
        node = node.children.get(string.charAt(index));
        if (node == null) {
          return null;
        }
      }
      return node.keywordConstructor;
    }

    final void register(final String keywordValue, final Constructor<? extends Lexical.Keyword> keywordConstructor) {
      this.register(keywordValue, 0, this.root, keywordConstructor);
    }

    private void register(
        final String keywordValue,
        final int characterIndex,
        final Node parent,
        final Constructor<? extends Lexical.Keyword> keywordConstructor
    ) {
      if (characterIndex == keywordValue.length()) {
        if (parent.keywordConstructor != null) {
          // TODO 重名了
          throw new UnsupportedOperationException();
        }
        parent.keywordConstructor = keywordConstructor;
      } else {
        final char character = keywordValue.charAt(characterIndex);
        if (this.caseSensitive) {
          this.register(keywordValue, characterIndex, character, parent, keywordConstructor);
        } else {
          this.register(keywordValue, characterIndex, Character.toLowerCase(character), parent, keywordConstructor);
          this.register(keywordValue, characterIndex, Character.toUpperCase(character), parent, keywordConstructor);
        }
      }
    }

    private void register(
        final String keywordValue,
        final int characterIndex,
        final char character,
        final Node parent,
        final Constructor<? extends Lexical.Keyword> keywordConstructor
    ) {
      Node node = parent.children.get(character);
      if (node == null) {
        node = new Node();
        parent.children.put(character, node);
      }
      this.register(keywordValue, characterIndex + 1, node, keywordConstructor);
    }

    private static final class Node {

      final Map<Character, Node> children = new HashMap<>(1);

      Constructor<? extends Lexical.Keyword> keywordConstructor;

    }

  }

}
