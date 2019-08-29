package com.huawei.cloudtable.leo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static com.huawei.cloudtable.leo.Table.Schema.Attribute.EMPTY_LIST;

public abstract class Table implements Closeable {

  public static final class Schema {

    private Schema(final Attribute<?>[] attributeList) {
      for (int attributeIndex = 0; attributeIndex < attributeList.length; attributeIndex++) {
        attributeList[attributeIndex].index = attributeIndex;
      }
      this.attributeList = attributeList;

    }

    private final Attribute<?>[] attributeList;

    public int getAttributeCount() {
      return this.attributeList.length;
    }

    public Attribute<?> getAttribute(final int attributeIndex) {
      return this.attributeList[attributeIndex];
    }

    public Integer getAttributeIndex(final Attribute<?> attribute) {
      try {
        if (this.attributeList[attribute.index] != attribute) {
          return null;
        } else {
          return attribute.index;
        }
      } catch (IndexOutOfBoundsException exception) {
        return null;
      }
    }

    public boolean equals(final Schema that) {
      if (this.attributeList.length != that.attributeList.length) {
        return false;
      }
      for (int index = 0; index < this.attributeList.length; index++) {
        final Attribute<?> thisAttribute = this.attributeList[index];
        final Attribute<?> thatAttribute = that.attributeList[index];
        if (thisAttribute.getValueClass() != thatAttribute.getValueClass()
            || thisAttribute.isNullable() != thatAttribute.isNullable()) {
          return false;
        }
      }
      return true;
    }

    public static final class Attribute<TValue> {

      static final Attribute[] EMPTY_LIST = new Attribute[0];

      private Attribute(final Class<TValue> valueClass, final boolean nullable) {
        this.valueClass = valueClass;
        this.nullable = nullable;
        this.index = -1;
      }

      private final Class<TValue> valueClass;

      private final boolean nullable;

      private int index;

      public Class<TValue> getValueClass() {
        return this.valueClass;
      }

      public boolean isNullable() {
        return this.nullable;
      }

    }

  }

  public static final class SchemaBuilder {

    public SchemaBuilder() {
      this.attributeList = new ArrayList<>();
    }

    private final List<Schema.Attribute<?>> attributeList;

    public void reset() {
      this.attributeList.clear();
    }

    public <TValue> void addAttribute(final Class<TValue> valueClass, final boolean nullable) {
      if (valueClass == null) {
        throw new IllegalArgumentException("Argument [valueClass] is null.");
      }
      this.attributeList.add(new Schema.Attribute<>(valueClass, nullable));
    }

    public Schema build() {
      if (this.attributeList.isEmpty()) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return new Schema(this.attributeList.toArray(EMPTY_LIST));
    }

  }

  public static final class Order {

    public Order(final Schema.Attribute<?> attribute, final boolean ascending) {
      if (attribute == null) {
        throw new IllegalArgumentException("Variable [attribute] is null.");
      }
      this.attribute = attribute;
      this.ascending = ascending;
    }

    private final Schema.Attribute<?> attribute;

    private final boolean ascending;

    public Schema.Attribute<?> getAttribute() {
      return this.attribute;
    }

    public boolean isAscending() {
      return this.ascending;
    }

  }

  public Table(final Schema schema) {
    this(schema, Collections.emptyList());
  }

  public Table(final Schema schema, final List<Order> orderBy) {
    if (schema == null) {
      throw new IllegalArgumentException("Argument [schema] is null.");
    }
    if (!orderBy.isEmpty()) {
      for (int orderIndex = 0; orderIndex < orderBy.size(); orderIndex++) {
        if (schema.getAttributeIndex(orderBy.get(orderIndex).attribute) == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
    }
    this.schema = schema;
    this.orderBy = Collections.unmodifiableList(orderBy);
  }

  private final Schema schema;

  private final List<Order> orderBy;

  public Schema getSchema() {
    return this.schema;
  }

  public List<Order> getOrderBy() {
    return this.orderBy;
  }

  public abstract boolean next() throws IOException;

  public <TValue> TValue get(final Schema.Attribute<TValue> attribute) {
    final Integer attributeIndex = this.schema.getAttributeIndex(attribute);
    if (attributeIndex == null) {
      // TODO
      throw new UnsupportedOperationException();
    }
    return attribute.getValueClass().cast(this.get(attributeIndex));
  }

  /**
   * @throws NoSuchElementException
   */
  public abstract Object get(int attributeIndex);

  /**
   * @throws NoSuchElementException
   * @throws ClassCastException
   */
  public abstract <TValue> TValue get(int attributeIndex, Class<TValue> resultClass);

}
