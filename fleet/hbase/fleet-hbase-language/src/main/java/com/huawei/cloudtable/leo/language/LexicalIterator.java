package com.huawei.cloudtable.leo.language;

abstract class LexicalIterator {

  abstract String getString();

  abstract void toHead();

  abstract void toTail();

  abstract Lexical prev(Lexical lexical);

  abstract Lexical prev();

  abstract Lexical next(Lexical lexical);

  abstract Lexical next();

}
