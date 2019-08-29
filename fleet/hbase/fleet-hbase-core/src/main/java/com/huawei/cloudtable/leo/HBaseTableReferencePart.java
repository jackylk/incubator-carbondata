package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.common.BytesHelper;
import com.huawei.cloudtable.leo.expression.Evaluation;

public abstract class HBaseTableReferencePart {

  public abstract PrimaryKey getPrimaryKey();

  public abstract int getColumnCount();

  public abstract ColumnReference getColumn(int columnIndexInTable);

  public abstract ColumnReference getColumn(ColumnIdentifier columnIdentifier);

  public static class PrimaryKey {

    PrimaryKey(final HBaseRowKeyCodecType codecType, final HBaseRowKeyDefinition definition) {
      this.codecType = codecType;
      this.definition = definition;
    }

    private final HBaseRowKeyCodecType codecType;

    private final HBaseRowKeyDefinition definition;

    public HBaseRowKeyCodecType getCodecType() {
      return this.codecType;
    }

    public HBaseRowKeyDefinition getDefinition() {
      return this.definition;
    }

  }

  public static final class ColumnIdentifier {

    public ColumnIdentifier() {
      // to do nothing.
    }

    public ColumnIdentifier(final byte[] family, final byte[] qualifier) {
      this.set(family, qualifier);
    }

    private byte[] family;

    private int familyOffset;

    private int familyLength;

    private byte[] qualifier;

    private int qualifierOffset;

    private int qualifierLength;

    private int hashCode;

    public void set(final byte[] family, final byte[] qualifier) {
      this.set(family, 0, family.length, qualifier, 0, qualifier.length);
    }

    public void set(
        final byte[] family,
        final int familyOffset,
        final int familyLength,
        final byte[] qualifier,
        final int qualifierOffset,
        final int qualifierLength
    ) {
      this.family = family;
      this.familyOffset = familyOffset;
      this.familyLength = familyLength;
      this.qualifier = qualifier;
      this.qualifierOffset = qualifierOffset;
      this.qualifierLength = qualifierLength;
      this.hashCode = 0;
    }

    @Override
    public int hashCode() {
      if (this.hashCode == 0) {
        int hashCode = 0;
        final int familyEnd = this.familyOffset + this.familyLength;
        for(int index = this.familyOffset; index < familyEnd; ++index) {
          hashCode = 31 * hashCode + this.family[index];
        }
        final int qualifierEnd = this.qualifierOffset + this.qualifierLength;
        for(int index = this.qualifierOffset; index < qualifierEnd; ++index) {
          hashCode = 31 * hashCode + this.qualifier[index];
        }
        this.hashCode = hashCode;
      }
      return this.hashCode;
    }

    @Override
    public boolean equals(final Object object) {
      if (object instanceof ColumnIdentifier) {
        final ColumnIdentifier that = (ColumnIdentifier) object;
        return BytesHelper.equals(this.family, this.familyOffset, this.familyLength, that.family, that.familyOffset, that.familyLength)
            && BytesHelper.equals(this.qualifier, this.qualifierOffset, this.qualifierLength, that.qualifier, that.qualifierOffset, that.qualifierLength);
      }
      return false;
    }

  }

  public static final class ColumnReference {

    ColumnReference(final int indexInTable, final HBaseValueCodec codec, final Evaluation defaultValue, final boolean nullable) {
      this.indexInTable = indexInTable;
      this.codec = codec;
      this.defaultValue = defaultValue;
      this.nullable = nullable;
    }

    private final int indexInTable;

    private final HBaseValueCodec codec;

    private final Evaluation defaultValue;

    private final boolean nullable;

    public int getIndexInTable() {
      return this.indexInTable;
    }

    public HBaseValueCodec getCodec() {
      return this.codec;
    }

    public Evaluation getDefaultValue() {
      return this.defaultValue;
    }

    public boolean isNullable() {
      return this.nullable;
    }

  }

}
