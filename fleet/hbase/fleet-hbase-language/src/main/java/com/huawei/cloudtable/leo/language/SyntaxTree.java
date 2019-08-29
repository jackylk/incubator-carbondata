package com.huawei.cloudtable.leo.language;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class SyntaxTree<TRoot extends SyntaxTree.Node> {

  SyntaxTree(final TRoot root) {
    this.root = root;
  }

  private final TRoot root;

  public TRoot getRoot() {
    return this.root;
  }

  public static abstract class Node {

    public Node() {
      // to do nothing.
    }

    private NodePosition position;

    /**
     * 由语法解析器调用
     */
    void setPosition(final NodePosition position) {
      this.position = position;
    }

    public NodePosition getPosition() {
      return this.position;
    }

    @Override
    public String toString() {
      final StringBuilder stringBuilder = new StringBuilder();
      this.toString(stringBuilder);
      return stringBuilder.toString();
    }

    public void toString(StringBuilder stringBuilder) {
      // to do nothing.
    }

  }

  public static final class NodeList<TNode extends Node, TDelimiter extends Lexical> extends Node implements Iterable<TNode> {

    public NodeList(final TNode element) {
      if (element == null) {
        throw new IllegalArgumentException("Argument [element] is null.");
      }
      this.elementList = Collections.unmodifiableList(Collections.singletonList(element));
      this.delimiter = null;
      this.hashCode = 0;
    }

    public NodeList(final List<TNode> elementList, final TDelimiter delimiter) {
      if (elementList == null) {
        throw new IllegalArgumentException("Argument [elementList] is null.");
      }
      if (elementList.isEmpty()) {
        throw new IllegalArgumentException("Argument [elementList] is empty.");
      }
      if (delimiter == null) {
        throw new IllegalArgumentException("Argument [delimiter] is null.");
      }
      this.elementList = Collections.unmodifiableList(elementList);
      this.delimiter = delimiter;
      this.hashCode = 0;
    }

    private final List<TNode> elementList;

    private final TDelimiter delimiter;

    private volatile int hashCode;

    public TNode get(final int index) {
      return this.elementList.get(index);
    }

    public TDelimiter getDelimiter() {
      return this.delimiter;
    }

    public boolean isEmpty() {
      return this.size() == 0;
    }

    public int size() {
      return this.elementList.size();
    }

    @Override
    public Iterator<TNode> iterator() {
      return this.elementList.iterator();
    }

    @Override
    public int hashCode() {
      if (this.hashCode == 0) {
        int hashCode = 0;
        for (TNode element : this.elementList) {
          hashCode = 31 * hashCode + element.hashCode();
        }
        this.hashCode = hashCode;
      }
      return this.hashCode;
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null) {
        return false;
      }
      if (object.getClass() == this.getClass()) {
        final NodeList that = (NodeList) object;
        if (this.elementList.size() == that.elementList.size()) {
          final int elementCount = this.elementList.size();
          for (int elementIndex = 0; elementIndex < elementCount; elementIndex++) {
            if (!this.elementList.get(elementIndex).equals(that.elementList.get(elementIndex))) {
              return false;
            }
          }
          return true;
        }
      }
      return false;
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      final int elementCount = this.elementList.size();
      for (int elementIndex = 0; elementIndex < elementCount; elementIndex++) {
        if (elementIndex != 0) {
          this.delimiter.toString(stringBuilder);
        }
        this.elementList.get(elementIndex).toString(stringBuilder);
      }
    }

  }

  public static final class NodePosition {

    public static NodePosition of(final int column) {
      return of(column, column);
    }

    public static NodePosition of(final int startColumn, final int endColumn) {
      if (endColumn < startColumn) {
        throw new IllegalArgumentException("Argument [end] is not greater than argument [start].");
      }
      if (startColumn < CACHE_SIZE && endColumn < CACHE_SIZE) {
        NodePosition[] cache = CACHE[startColumn];
        if (cache == null) {
          synchronized (CACHE) {
            cache = CACHE[startColumn];
            if (cache == null) {
              cache = new NodePosition[CACHE_SIZE];
              CACHE[startColumn] = cache;
            }
          }
        }
        NodePosition position = cache[endColumn];
        if (position == null) {
          synchronized (CACHE) {
            position = cache[endColumn];
            if (position == null) {
              position = new NodePosition(startColumn, endColumn);
              cache[endColumn] = position;
            }
          }
        }
        return position;
      } else {
        return new NodePosition(startColumn, endColumn);
      }
    }

    private static final int CACHE_SIZE = 256;

    private static final NodePosition[][] CACHE = new NodePosition[CACHE_SIZE][];

    private NodePosition(final int start, final int end) {
      this.start = start;
      this.end = end;
    }

    private final int start;

    private final int end;

    public int getStart() {
      return this.start;
    }

    public int getEnd() {
      return this.end;
    }

    public String getString(final String string) {
      return string.substring(this.start, this.end + 1);
    }

    @Override
    public int hashCode() {
      return 31 * this.start + this.end;
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object instanceof NodePosition) {
        final NodePosition that = (NodePosition) object;
        return this.start == that.start && this.end == that.end;
      }
      return false;
    }

    @Override
    public String toString() {
      return "[" + this.start + ":" + this.end + "]";
    }

  }


}
