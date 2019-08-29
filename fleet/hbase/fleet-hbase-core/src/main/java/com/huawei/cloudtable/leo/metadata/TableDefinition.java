package com.huawei.cloudtable.leo.metadata;

import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueType;
import com.huawei.cloudtable.leo.expression.Evaluation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * The definition object build database table.
 */
public class TableDefinition {

  public static final byte NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

  public static final String NAME_PATTERN = "[0-9a-zA-Z_]{1," + NAME_MAXIMUM_LENGTH + "}";

  public static final short DESCRIPTION_MAXIMUM_LENGTH = Short.MAX_VALUE;

  public static final String DESCRIPTION_PATTERN = "[\\S\\s]{0," + DESCRIPTION_MAXIMUM_LENGTH + "}";

  public static final byte PROPERTY_NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

  public static final short PROPERTY_VALUE_MAXIMUM_LENGTH = Short.MAX_VALUE;

  private TableDefinition(
      final Identifier name,
      final List<Column<?>> columnList,
      final Map<Identifier, Column<?>> columnMapByName,
      final List<Constraint> constraintList,
      final String description,
      final Map<Identifier, String> properties
  ) {
    int columnIndex = 0;
    for (Column<?> column : columnList) {
      column.indexInTable = columnIndex++;
    }
    this.name = name;
    this.primaryKey = getPrimaryKey(constraintList);
    this.columnList = Collections.unmodifiableList(columnList);
    this.columnMapByName = columnMapByName;
    this.constraintList = Collections.unmodifiableList(constraintList);
    this.description = description;
    this.properties = Collections.unmodifiableMap(properties);
  }

  private final Identifier name;

  private final PrimaryKey primaryKey;

  private final List<Column<?>> columnList;

  private final Map<Identifier, Column<?>> columnMapByName;

  private final List<Constraint> constraintList;

  private final String description;

  private final Map<Identifier, String> properties;

  @Nonnull
  public Identifier getName() {
    return this.name;
  }

  @Nullable
  public PrimaryKey getPrimaryKey() {
    return this.primaryKey;
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

  public Column<?> getColumn(final Identifier columnName) {
    return this.columnMapByName.get(columnName);
  }

  public Column<?> getColumn(final int columnIndex) {
    return this.columnList.get(columnIndex);
  }

  public Integer getColumnIndex(final Identifier columnName) {
    final Column<?> column = this.getColumn(columnName);
    return column == null ? null : column.indexInTable;
  }

  public Integer getColumnIndex(final Column<?> column) {
    // judge index range first
    if (column.indexInTable >= 0 && column.indexInTable < this.columnList.size()) {
      if (this.columnList.get(column.indexInTable) != column) {
        return null;
      } else {
        return column.indexInTable;
      }
    } else {
      return null;
    }
  }

  public List<Constraint> getConstraintList() {
    return this.constraintList;
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

  private static PrimaryKey getPrimaryKey(final List<Constraint> constraintList) {
    for (Constraint constraint : constraintList) {
      if (PrimaryKey.class == constraint.getClass()) {
        return PrimaryKey.class.cast(constraint);
      }
    }
    return null;
  }

  private static PrimaryKey setPrimaryKey(final List<Constraint> constraintList, final PrimaryKey newPrimaryKey) {
    int constraintIndex = 0;
    for (; constraintIndex < constraintList.size(); constraintIndex++) {
      final Constraint constraint = constraintList.get(constraintIndex);
      if (PrimaryKey.class == constraint.getClass()) {
        break;
      }
    }
    if (constraintIndex < constraintList.size()) {
      // Found.
      return PrimaryKey.class.cast(constraintList.set(constraintIndex, newPrimaryKey));
    } else {
      // Not found.
      constraintList.add(newPrimaryKey);
      return null;
    }
  }

  public static abstract class Constraint {

    public static final byte NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

    Constraint(final Identifier name) {
      this.name = name;
    }

    private final Identifier name;

    public Identifier getName() {
      return this.name;
    }

//    abstract boolean check();

  }

  public static final class PrimaryKey extends Constraint {

    PrimaryKey(final Identifier name, final List<Column<?>> columnList) {
      super(name);
      final Map<Identifier, Column<?>> columnMapByName = new HashMap<>(columnList.size());
      for (int columnIndex = 0; columnIndex < columnList.size(); columnIndex++) {
        final Column<?> column = columnList.get(columnIndex);
        columnMapByName.put(column.getName(), column);
        column.indexInPrimaryKey = columnIndex;
      }
      this.columnList = Collections.unmodifiableList(columnList);
      this.columnMapByName = columnMapByName;
      this.columnMaximumIndex = columnList.size() - 1;
    }

    private final List<Column<?>> columnList;

    private final Map<Identifier, Column<?>> columnMapByName;

    private final int columnMaximumIndex;

    public List<Column<?>> getColumnList() {
      return this.columnList;
    }

    public int getColumnCount() {
      return this.columnList.size();
    }

    public Column<?> getColumn(final int columnIndex) {
      return this.columnList.get(columnIndex);
    }

    public Column<?> getColumn(final Identifier columnName) {
      return this.columnMapByName.get(columnName);
    }

    public Column<?> getTheFirstColumn() {
      return this.columnList.get(0);
    }

    public Column<?> getTheLastColumn() {
      return this.columnList.get(this.columnMaximumIndex);
    }

    public Integer getColumnIndex(final Identifier columnName) {
      final Column<?> column = this.getColumn(columnName);
      return column == null ? null : column.indexInPrimaryKey;
    }

    public Integer getColumnIndex(final Column<?> column) {
      // judge index range first
      if (column.indexInPrimaryKey >= 0 && column.indexInPrimaryKey < this.columnList.size()) {
        if (this.columnList.get(column.indexInPrimaryKey) != column) {
          return null;
        } else {
          return column.indexInPrimaryKey;
        }
      } else {
        return null;
      }
    }

    public boolean isTheFirstColumn(final Identifier columnName) {
      final Integer columnIndex = this.getColumnIndex(columnName);
      if (columnIndex == null) {
        // TODO
        throw new UnsupportedOperationException();
      } else {
        return columnIndex == 0;
      }
    }

    public boolean isTheFirstColumn(final Column<?> column) {
      final Integer columnIndex = this.getColumnIndex(column);
      if (columnIndex == null) {
        // TODO
        throw new UnsupportedOperationException();
      } else {
        return columnIndex == 0;
      }
    }

    public boolean isTheLastColumn(final Identifier columnName) {
      final Integer columnIndex = this.getColumnIndex(columnName);
      if (columnIndex == null) {
        // TODO
        throw new UnsupportedOperationException();
      } else {
        return columnIndex == this.columnMaximumIndex;
      }
    }

    public boolean isTheLastColumn(final Column<?> column) {
      final Integer columnIndex = this.getColumnIndex(column);
      if (columnIndex == null) {
        // TODO
        throw new UnsupportedOperationException();
      } else {
        return columnIndex == this.columnMaximumIndex;
      }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public boolean isTheLastColumn(final int columnIndex) {
      this.getColumn(columnIndex);// check argument.
      return columnIndex == this.columnMaximumIndex;
    }

  }

  public static final class Column<TValue> {

    public static final byte NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

    public static final short DESCRIPTION_MAXIMUM_LENGTH = Short.MAX_VALUE;

    public static final String DESCRIPTION_PATTERN = "[\\S\\s]{0," + DESCRIPTION_MAXIMUM_LENGTH + "}";

    public static final byte PROPERTY_NAME_MAXIMUM_LENGTH = Byte.MAX_VALUE;

    public static final short PROPERTY_VALUE_MAXIMUM_LENGTH = Short.MAX_VALUE;

    private Column(
        final Identifier name,
        final ValueType<TValue> valueType,
        final boolean nullable,
        final Evaluation<TValue> defaultValue,
        final String description,
        final Map<Identifier, String> properties
    ) {
      this.name = name;
      this.valueType = valueType;
      this.nullable = nullable;
      this.defaultValue = defaultValue;
      this.description = description;
      this.properties = properties;
      this.indexInTable = -1;
      this.indexInPrimaryKey = -1;
    }

    private final Identifier name;

    private final ValueType<TValue> valueType;

    private final boolean nullable;

    private final Evaluation<TValue> defaultValue;

    private final String description;

    private final Map<Identifier, String> properties;

    private int indexInTable;

    private int indexInPrimaryKey;

    public Identifier getName() {
      return this.name;
    }

    public ValueType<TValue> getValueType() {
      return this.valueType;
    }

    public Class<TValue> getValueClass() {
      return this.valueType.getClazz();
    }

    public Evaluation<TValue> getDefaultValue() {
      return this.defaultValue;
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

    public boolean isNullable() {
      return this.nullable;
    }

    public int getIndexInTable() {
      return indexInTable;
    }

    @Override
    public int hashCode() {
      return this.name.hashCode();
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
      return this.name.equals(that.name);
    }

  }

  public static final class ColumnProperty {

    /**
     * Byte count.
     */
    public static final Identifier FIXED_LENGTH = Identifier.of("LEMON.FIXED_LENGTH");

    /**
     * Byte count.
     */
    public static final Identifier MAXIMUM_LENGTH = Identifier.of("LEMON.MAXIMUM_LENGTH");

    public static final Identifier PRECISION = Identifier.of("LEMON.PRECISION");

    public static final Identifier SCALE = Identifier.of("LEMON.SCALE");

    public static final Identifier CHARSET = Identifier.of("LEMON.CHARSET");

    // TODO STORAGE_TYPE 存储方式，值存储到列名中（打横存储）、普通存储

    private ColumnProperty() {
      // to do nothing.
    }

  }

  public static final class Builder {

    public Builder(final TableDefinition based) {
      // TODO
      throw new UnsupportedOperationException();
    }

    public Builder(final Identifier tableName) {
      if (tableName == null) {
        throw new IllegalArgumentException("Argument [tableName] is null.");
      }
      if (!tableName.matches(NAME_PATTERN)) {
        throw new IllegalArgumentException("Argument [tableName] is not matches " + NAME_PATTERN + ".");
      }
      this.tableName = tableName;
      this.tableColumnList = new ArrayList<>();
      this.tableColumnMapByName = new HashMap<>();
      this.tableConstraintList = new ArrayList<>();
      this.tableProperties = new HashMap<>();
    }

    private final Identifier tableName;

    private final List<Column<?>> tableColumnList;

    private final Map<Identifier, Column<?>> tableColumnMapByName;

    private final List<Constraint> tableConstraintList;

    private String tableDescription;

    private final Map<Identifier, String> tableProperties;

    public <TValue> Column<TValue> addColumn(
        final Identifier columnName,
        final ValueType<TValue> columnValueType,
        final boolean columnNullable,
        final Evaluation<TValue> columnDefaultValue,
        final String columnDescription
    ) {
      return this.addColumn(
          columnName,
          columnValueType,
          columnNullable,
          columnDefaultValue,
          columnDescription,
          Collections.emptyMap()
      );
    }

    public <TValue> Column<TValue> addColumn(
        final Identifier columnName,
        final ValueType<TValue> columnValueType,
        final boolean columnNullable,
        final Evaluation<TValue> columnDefaultValue,
        final String columnDescription,
        final Map<Identifier, String> columnProperties
    ) {
      if (columnName.length() > Column.NAME_MAXIMUM_LENGTH) {
        throw new IllegalArgumentException(
            "Argument [columnName] is too long, the maximum getLength is " + Column.NAME_MAXIMUM_LENGTH + "."
        );
      }
      if (columnValueType == null) {
        throw new IllegalArgumentException("Argument [columnValueType] is null.");
      }
      if (columnDescription != null && !columnDescription.matches(Column.DESCRIPTION_PATTERN)) {
        throw new IllegalArgumentException(
            "Argument [columnDescription] is not matches " + Column.DESCRIPTION_PATTERN + "."
        );
      }
      if (columnProperties == null) {
        throw new IllegalArgumentException("Argument [columnProperties] is null.");
      }
      if (!columnProperties.isEmpty()) {
        for (Map.Entry<Identifier, String> columnProperty : columnProperties.entrySet()) {
          final Identifier propertyName = columnProperty.getKey();
          final String propertyValue = columnProperty.getValue();
          if (propertyName.length() > Column.PROPERTY_NAME_MAXIMUM_LENGTH) {
            throw new IllegalArgumentException(
                "Argument [columnPropertyName] is too long, the maximum getLength is " + Column.PROPERTY_NAME_MAXIMUM_LENGTH + "."
            );
          }
          if (propertyValue != null && propertyValue.length() > Column.PROPERTY_VALUE_MAXIMUM_LENGTH) {
            throw new IllegalArgumentException(
                "Argument [columnPropertyValue] is too long, the maximum getLength is " + Column.PROPERTY_VALUE_MAXIMUM_LENGTH + "."
            );
          }
        }
      }
      if (columnDefaultValue != null && columnDefaultValue.getResultClass() != columnValueType.getClazz()) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (this.tableColumnMapByName.containsKey(columnName)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      final Column<TValue> newColumn = new Column<>(
          columnName,
          columnValueType,
          columnNullable,
          columnDefaultValue,
          columnDescription,
          columnProperties
      );
      this.tableColumnList.add(newColumn);
      this.tableColumnMapByName.put(columnName, newColumn);
      return newColumn;
    }

    public PrimaryKey setPrimaryKey(final Identifier primaryKeyName, final Identifier... columnNameList) {
      if (primaryKeyName.length() > Constraint.NAME_MAXIMUM_LENGTH) {
        throw new IllegalArgumentException(
            "Argument [primaryKeyName] is too long, the maximum getLength is " + Constraint.NAME_MAXIMUM_LENGTH + "."
        );
      }
      if (columnNameList.length == 0) {
        throw new IllegalArgumentException("Argument [columnNameList] is empty.");
      }
      final List<Column<?>> columnList = new ArrayList<>(columnNameList.length);
      final Set<Identifier> columnNames = new HashSet<>(columnNameList.length);
      for (Identifier columnName : columnNameList) {
        if (columnNames.contains(columnName)) {
          // TODO
          throw new UnsupportedOperationException();
        }
        final Column<?> column = this.tableColumnMapByName.get(columnName);
        if (column == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        columnList.add(column);
        columnNames.add(columnName);
      }
      return TableDefinition.setPrimaryKey(this.tableConstraintList, new PrimaryKey(primaryKeyName, columnList));
    }

    public PrimaryKey setPrimaryKey(final Identifier primaryKeyName, final List<Identifier> columnNameList) {
      if (primaryKeyName.length() > Constraint.NAME_MAXIMUM_LENGTH) {
        throw new IllegalArgumentException(
            "Argument [primaryKeyName] is too long, the maximum getLength is " + Constraint.NAME_MAXIMUM_LENGTH + "."
        );
      }
      if (columnNameList.isEmpty()) {
        throw new IllegalArgumentException("Argument [columnNameList] is empty.");
      }
      final List<Column<?>> columnList = new ArrayList<>(columnNameList.size());
      final Set<Identifier> columnNames = new HashSet<>(columnNameList.size());
      for (Identifier columnName : columnNameList) {
        if (columnNames.contains(columnName)) {
          // TODO
          throw new UnsupportedOperationException();
        }
        final Column<?> column = this.tableColumnMapByName.get(columnName);
        if (column == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        columnList.add(column);
        columnNames.add(columnName);
      }
      return TableDefinition.setPrimaryKey(this.tableConstraintList, new PrimaryKey(primaryKeyName, columnList));
    }

    // Define other constraints.

    public String setDescription(final String description) {
      if (description != null && !description.matches(TableDefinition.DESCRIPTION_PATTERN)) {
        throw new IllegalArgumentException(
            "Argument [description] is not matches " + TableDefinition.DESCRIPTION_PATTERN + "."
        );
      }
      final String oldDescription = this.tableDescription;
      this.tableDescription = description;
      return oldDescription;
    }

    public String setProperty(final Identifier propertyName, final String propertyValue) {
      if (propertyName.length() > TableDefinition.PROPERTY_NAME_MAXIMUM_LENGTH) {
        throw new IllegalArgumentException(
            "Argument [propertyName] is too long, the maximum getLength is " + TableDefinition.PROPERTY_NAME_MAXIMUM_LENGTH + "."
        );
      }
      if (propertyValue == null) {
        return this.tableProperties.remove(propertyName);
      } else {
        if (propertyValue.length() > TableDefinition.PROPERTY_VALUE_MAXIMUM_LENGTH) {
          throw new IllegalArgumentException(
              "Argument [propertyValue] is too long, the maximum getLength is " + TableDefinition.PROPERTY_VALUE_MAXIMUM_LENGTH + "."
          );
        }
        return this.tableProperties.put(propertyName, propertyValue);
      }
    }

    public Column<?> removeColumn(final Identifier columnName) {
      // TODO
      throw new UnsupportedOperationException();
    }

    public TableDefinition build() {
      return new TableDefinition(
          this.tableName,
          this.tableColumnList,
          this.tableColumnMapByName,
          this.tableConstraintList,
          this.tableDescription,
          this.tableProperties.isEmpty() ? Collections.emptyMap() : this.tableProperties
      );
    }

  }

}
