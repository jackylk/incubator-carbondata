package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.metadata.TableDefinition;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseTableColumnReference<TValue> {

  public HBaseTableColumnReference(final TableDefinition.Column<TValue> column, final HBaseValueCodec<TValue> codec) {
    if (column == null) {
      throw new IllegalArgumentException("Argument [column] is null.");
    }
    if (codec == null) {
      throw new IllegalArgumentException("Argument [codec] is null.");
    }
    final HBaseIdentifier name;
    if (column.getName() instanceof HBaseIdentifier) {
      name = (HBaseIdentifier) column.getName();
    } else {
      name = HBaseIdentifier.of(column.getName().toString());
    }
    this.column = column;
    this.name = name;
    this.codec = codec;
    this.family = null;
    this.qualifier = null;
  }

  public HBaseTableColumnReference(
      final TableDefinition.Column<TValue> column,
      final HBaseValueCodec<TValue> codec,
      final HBaseIdentifier family,
      final HBaseIdentifier qualifier
  ) {
    if (column == null) {
      throw new IllegalArgumentException("Argument [column] is null.");
    }
    if (codec == null) {
      throw new IllegalArgumentException("Argument [codec] is null.");
    }
    if (family == null) {
      throw new IllegalArgumentException("Argument [family] is null.");
    }
    final HBaseIdentifier name;
    if (column.getName() instanceof HBaseIdentifier) {
      name = (HBaseIdentifier) column.getName();
    } else {
      name = HBaseIdentifier.of(column.getName().toString());
    }
    this.column = column;
    this.name = name;
    this.codec = codec;
    this.family = family;
    this.qualifier = qualifier == null ? name : qualifier;
  }

  private final TableDefinition.Column<TValue> column;

  private final HBaseIdentifier name;

  private final HBaseValueCodec<TValue> codec;

  private final HBaseIdentifier family;

  private final HBaseIdentifier qualifier;

  public TableDefinition.Column<TValue> getColumn() {
    return this.column;
  }

  public HBaseIdentifier getName() {
    return this.name;
  }

  public HBaseValueCodec<TValue> getCodec() {
    return this.codec;
  }

  public HBaseIdentifier getFamily() {
    return this.family;
  }

  public HBaseIdentifier getQualifier() {
    return this.qualifier;
  }

  public byte[] getValue(final Result result) {
    return result.getValue(this.family.getBytes(), this.qualifier.getBytes());
  }

  public void setValue(final Put put, final byte[] value) {
    put.addColumn(this.family.getBytes(), this.qualifier.getBytes(), value);
  }

  public void setTo(final Get get) {
    get.addColumn(this.family.getBytes(), this.qualifier.getBytes());
  }

  public void setTo(final Scan scan) {
    scan.addColumn(this.family.getBytes(), this.qualifier.getBytes());
  }

}
