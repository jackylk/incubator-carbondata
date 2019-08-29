package com.huawei.cloudtable.leo.metadata;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.expression.Evaluation;

import java.util.*;

/**
 * The definition object build index.
 */
public class IndexDefinition {

  public static final byte NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

  public static final String NAME_PATTERN = "[0-9a-zA-Z_]{1," + NAME_MAXIMUM_LENGTH + "}";

  public static final short DESCRIPTION_MAXIMUM_LENGTH = Short.MAX_VALUE;

  public static final String DESCRIPTION_PATTERN = "[\\S\\s]{0," + DESCRIPTION_MAXIMUM_LENGTH + "}";

  public static final byte PROPERTY_NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

  public static final short PROPERTY_VALUE_MAXIMUM_LENGTH = Short.MAX_VALUE;

  private IndexDefinition(
      final Identifier name,
      final Type type,
      final List<Column<?>> columnList,
      final String description,
      final Map<Identifier, String> properties
  ) {
    int columnIndex = 0;
    for (Column<?> column : columnList) {
      column.index = columnIndex++;
    }
    this.name = name;
    this.type = type;
    this.columnList = Collections.unmodifiableList(columnList);
    this.description = description;
    this.properties = Collections.unmodifiableMap(properties);
  }

  private final Identifier name;

  private final Type type;

  private final List<Column<?>> columnList;

  private final String description;

  private final Map<Identifier, String> properties;

  public Identifier getName() {
    return this.name;
  }

  public Type getType() {
    return this.type;
  }

  public int getColumnCount() {
    return this.columnList.size();
  }

  /**
   * Ordered.
   */
  public List<Column<?>> getColumnList() {
    return this.columnList;
  }

  public Column<?> getColumn(final int columnIndex) {
    return this.columnList.get(columnIndex);
  }

  public Integer getColumnIndex(final Column<?> column) {
    try {
      if (this.columnList.get(column.index) != column) {
        return null;
      } else {
        return column.index;
      }
    } catch (IndexOutOfBoundsException exception) {
      return null;
    }
  }

  public String getDescription() {
    return this.description;
  }

  public Map<Identifier, String> getProperties() {
    return this.properties;
  }

  public String getProperty(final Identifier propertyName) {
    return this.properties.get(propertyName);
  }

  public enum Type {

    B_TREE,

    BITMAP

  }

  public static final class Column<TValue> {

    private Column(final Evaluation<TValue> expression) {
      this.expression = expression;
    }

    int index;

    private final Evaluation<TValue> expression;

    public Evaluation<TValue> getExpression() {
      return this.expression;
    }

    @Override
    public int hashCode() {
      return this.expression.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || this.getClass() != object.getClass()) {
        return false;
      }
      final Column<?> that = (Column<?>) object;
      return this.expression.equals(that.expression);
    }

    @Override
    public String toString() {
      return this.expression.toString();
    }

  }

  public static final class Builder {

    public Builder(final Identifier indexName, final Type indexType) {
      if (!indexName.matches(NAME_PATTERN)) {
        throw new IllegalArgumentException("Argument [indexName] is not matches " + NAME_PATTERN + ".");
      }
      if (indexType == null) {
        throw new IllegalArgumentException("Argument [indexType] is null.");
      }
      this.indexName = indexName;
      this.indexType = indexType;
      this.indexColumnList = new ArrayList<>();
      this.indexProperties = new HashMap<>();
    }

    private final Identifier indexName;

    private final Type indexType;

    private final List<Column<?>> indexColumnList;

    private String indexDescription;

    private Map<Identifier, String> indexProperties;

    public void addColumn(final Evaluation<?> indexColumn) {
      if (indexColumn == null) {
        throw new IllegalArgumentException("Argument [indexColumnDeclare] is null.");
      }
      // TODO indexColumn中不能有Variable和Reference.
      this.indexColumnList.add(new Column<>(indexColumn));
    }

    public String setDescription(final String description) {
      if (description != null && !description.matches(DESCRIPTION_PATTERN)) {
        throw new IllegalArgumentException(
            "Argument [description] is not matches " + DESCRIPTION_PATTERN + "."
        );
      }
      final String oldDescription = this.indexDescription;
      this.indexDescription = description;
      return oldDescription;
    }

    public String setProperty(final Identifier propertyName, final String propertyValue) {
      if (propertyName.length() > PROPERTY_NAME_MAXIMUM_LENGTH) {
        throw new IllegalArgumentException(
            "Argument [propertyName] is too long, the maximum getLength is " + PROPERTY_NAME_MAXIMUM_LENGTH + "."
        );
      }
      if (propertyValue == null) {
        if (this.indexProperties == null) {
          return null;
        } else {
          return this.indexProperties.remove(propertyName);
        }
      } else {
        if (propertyValue.length() > PROPERTY_VALUE_MAXIMUM_LENGTH) {
          throw new IllegalArgumentException(
              "Argument [propertyValue] is too long, the maximum getLength is " + PROPERTY_VALUE_MAXIMUM_LENGTH + "."
          );
        }
        if (this.indexProperties == null) {
          this.indexProperties = new HashMap<>();
        }
        return this.indexProperties.put(propertyName, propertyValue);
      }
    }

    public IndexDefinition build() {
      if (this.indexColumnList.isEmpty()) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return new IndexDefinition(
          this.indexName,
          this.indexType,
          this.indexColumnList,
          this.indexDescription,
          this.indexProperties == null ? Collections.emptyMap() : this.indexProperties
      );
    }

  }

}
