package com.huawei.cloudtable.leo;


import com.huawei.cloudtable.leo.HBaseTableReferencePart.ColumnIdentifier;
import com.huawei.cloudtable.leo.HBaseTableReferencePart.ColumnReference;
import com.huawei.cloudtable.leo.expression.*;
import com.huawei.cloudtable.leo.hbase.codecs.StringCodec;
import com.huawei.cloudtable.leo.metadata.TableDefinition;

import java.nio.ByteBuffer;
import java.util.*;

public final class HBaseTableReferenceCodec {

  private static final ThreadLocal<Set<Integer>> ATTRIBUTE_INDEXES_CACHE = new ThreadLocal<Set<Integer>>() {
    @Override
    protected Set<Integer> initialValue() {
      return new HashSet<>();
    }
  };

  private static final ThreadLocal<List<HBaseTableColumnReference>> COLUMN_REFERENCE_LIST_CACHE = new ThreadLocal<List<HBaseTableColumnReference>>() {
    @Override
    protected List<HBaseTableColumnReference> initialValue() {
      return new ArrayList<>();
    }
  };

  private static final StringCodec STRING_CODEC = new StringCodec.StringUTF8Codec();

  private static final byte VALUE_TRUE = 0;

  private static final byte VALUE_FALSE = 1;

  public static void encode(
      final HBaseTableReference tableReference,
      final Expression expression,
      final ExpressionCodec.EncodeContext encodeContext,
      final ByteBuffer byteBuffer
  ) {
    final Set<Integer> attributeIndexes = ATTRIBUTE_INDEXES_CACHE.get();
    try {
      AttributeIndexCollector.collect(expression, attributeIndexes);
      encode(tableReference, attributeIndexes, encodeContext, byteBuffer);
    } finally {
      attributeIndexes.clear();
    }
  }

  public static void encode(
      final HBaseTableReference tableReference,
      final List<? extends Expression> expressions,
      final ExpressionCodec.EncodeContext encodeContext,
      final ByteBuffer byteBuffer
  ) {
    final Set<Integer> attributeIndexes = ATTRIBUTE_INDEXES_CACHE.get();
    try {
      for (int index = 0; index < expressions.size(); index++) {
        AttributeIndexCollector.collect(expressions.get(index), attributeIndexes);
      }
      encode(tableReference, attributeIndexes, encodeContext, byteBuffer);
    } finally {
      attributeIndexes.clear();
    }
  }

  private static void encode(
      final HBaseTableReference tableReference,
      final Set<Integer> attributeIndexes,
      final ExpressionCodec.EncodeContext encodeContext,
      final ByteBuffer byteBuffer
  ) {
    final List<HBaseTableColumnReference> columnReferenceList = COLUMN_REFERENCE_LIST_CACHE.get();
    try {
      final TableDefinition.PrimaryKey primaryKey = tableReference.getPrimaryKey();
      int columnIndexInPrimaryKeyMaximum = -1;
      if (primaryKey != null) {
        for (Integer attributeIndex : attributeIndexes) {
          final HBaseTableColumnReference columnReference = tableReference.getColumnReference(attributeIndex);
          final Integer columnIndexInPrimaryKey = primaryKey.getColumnIndex(columnReference.getColumn());
          if (columnIndexInPrimaryKey != null) {
            columnIndexInPrimaryKeyMaximum = Math.max(columnIndexInPrimaryKeyMaximum, columnIndexInPrimaryKey);
          } else {
            columnReferenceList.add(columnReference);
          }
        }
      } else {
        for (Integer reference : attributeIndexes) {
          columnReferenceList.add(tableReference.getColumnReference(reference));
        }
      }
      byteBuffer.putInt(attributeIndexes.size());
      encodePrimaryKey(tableReference, columnIndexInPrimaryKeyMaximum + 1, attributeIndexes, encodeContext, byteBuffer);
      encodeColumnList(tableReference, columnReferenceList, encodeContext, byteBuffer);
    } finally {
      columnReferenceList.clear();
    }
  }

  private static void encodePrimaryKey(
      final HBaseTableReference tableReference,
      final int primaryKeyColumnCount,
      final Set<Integer> attributeIndexes,
      final ExpressionCodec.EncodeContext encodeContext,
      final ByteBuffer byteBuffer) {
    final TableDefinition.PrimaryKey primaryKey = tableReference.getPrimaryKey();
    byteBuffer.putInt(primaryKeyColumnCount);
    if (primaryKeyColumnCount > 0) {
      encodeString(tableReference.getPrimaryKeyCodecType().name(), byteBuffer);
      for (int index = 0; index < primaryKeyColumnCount; index++) {
        final int columnIndex = tableReference.getColumnIndex(primaryKey.getColumn(index));
        final boolean primaryKeyColumnUsedMark = attributeIndexes.contains(columnIndex);
        encodeColumn(tableReference, tableReference.getColumnReference(columnIndex), encodeContext, primaryKeyColumnUsedMark, byteBuffer);
        encodeBoolean(primaryKeyColumnUsedMark, byteBuffer);
      }
    }
  }

  private static void encodeColumnList(
      final HBaseTableReference tableReference,
      final List<HBaseTableColumnReference> columnReferenceList,
      final ExpressionCodec.EncodeContext encodeContext,
      final ByteBuffer byteBuffer
  ) {
    byteBuffer.putInt(columnReferenceList.size());
    for (HBaseTableColumnReference columnReference : columnReferenceList) {
      encodeColumn(tableReference, columnReference, encodeContext, true, byteBuffer);
      encodeIdentifier(columnReference.getFamily(), byteBuffer);
      encodeIdentifier(columnReference.getQualifier(), byteBuffer);
    }
  }

  private static void encodeColumn(
      final HBaseTableReference tableReference,
      final HBaseTableColumnReference columnReference,
      final ExpressionCodec.EncodeContext encodeContext,
      final boolean encodeDefaultValue,
      final ByteBuffer byteBuffer
  ) {
    byteBuffer.putInt(tableReference.getColumnIndex(columnReference.getColumn()));
    byteBuffer.putShort(columnReference.getCodec().index);
    encodeBoolean(columnReference.getColumn().isNullable(), byteBuffer);
    if (encodeDefaultValue) {
      ExpressionCodec.encode(encodeContext, columnReference.getColumn().getDefaultValue(), byteBuffer);
    } else {
      ExpressionCodec.encode(encodeContext, null, byteBuffer);
    }
  }

  private static void encodeBoolean(final boolean aBoolean, final ByteBuffer byteBuffer) {
    byteBuffer.put(aBoolean ? VALUE_TRUE : VALUE_FALSE);
  }

  private static void encodeString(final String string, final ByteBuffer byteBuffer) {
    STRING_CODEC.encodeWithLength(string, byteBuffer);
  }

  private static void encodeIdentifier(final HBaseIdentifier identifier, final ByteBuffer byteBuffer) {
    identifier.writeWithLength(byteBuffer);
  }

  public static HBaseTableReferencePart decode(
      final ByteBuffer byteBuffer,
      final ExpressionCodec.DecodeContext decodeContext,
      final HBaseValueCodecManager valueCodecManager
  ) {
    final int columnReferenceCount = byteBuffer.getInt();
    final HBaseTablePrimaryKey primaryKey = decodePrimaryKey(byteBuffer, decodeContext, valueCodecManager);
    final Map<Integer, ColumnReference> columnReferenceMapByIndex = new HashMap<>(columnReferenceCount);
    if (primaryKey != null) {
      final HBaseRowKeyDefinition primaryKeyDefinition = primaryKey.getDefinition();
      for (int index = 0; index < primaryKeyDefinition.getColumnCount(); index++) {
        if (primaryKey.primaryKeyColumnUsedMarkList[index]) {
          columnReferenceMapByIndex.put(primaryKeyDefinition.getColumnIndexInTable(index), primaryKey.primaryKeyColumnList[index]);
        }
      }
    }
    final Map<ColumnIdentifier, ColumnReference> columnReferenceMapByIdentifier = new HashMap<>(columnReferenceCount - columnReferenceMapByIndex.size());
    decodeColumnList(byteBuffer, decodeContext, valueCodecManager, columnReferenceMapByIndex, columnReferenceMapByIdentifier);
    return new HBaseTableReferencePart() {
      @Override
      public PrimaryKey getPrimaryKey() {
        return primaryKey;
      }

      @Override
      public int getColumnCount() {
        return columnReferenceMapByIndex.size();
      }

      @Override
      public ColumnReference getColumn(final int columnIndexInTable) {
        return columnReferenceMapByIndex.get(columnIndexInTable);
      }

      @Override
      public ColumnReference getColumn(final ColumnIdentifier columnIdentifier) {
        return columnReferenceMapByIdentifier.get(columnIdentifier);
      }
    };
  }

  private static HBaseTablePrimaryKey decodePrimaryKey(
      final ByteBuffer byteBuffer,
      final ExpressionCodec.DecodeContext decodeContext,
      final HBaseValueCodecManager valueCodecManager
  ) {
    final int primaryKeyColumnCount = byteBuffer.getInt();
    if (primaryKeyColumnCount == 0) {
      return null;
    }
    final HBaseRowKeyCodecType primaryKeyCodecType = HBaseRowKeyCodecType.valueOf(decodeString(byteBuffer));
    final ColumnReference[] primaryKeyColumnList = new ColumnReference[primaryKeyColumnCount];
    final boolean[] primaryKeyColumnUsedMarkList = new boolean[primaryKeyColumnCount];
    for (int index = 0; index < primaryKeyColumnCount; index++) {
      primaryKeyColumnList[index] = decodeColumn(byteBuffer, decodeContext, valueCodecManager);
      primaryKeyColumnUsedMarkList[index] = decodeBoolean(byteBuffer);
    }
    return new HBaseTablePrimaryKey(primaryKeyCodecType, primaryKeyColumnList, primaryKeyColumnUsedMarkList);
  }

  private static void decodeColumnList(
      final ByteBuffer byteBuffer,
      final ExpressionCodec.DecodeContext decodeContext,
      final HBaseValueCodecManager valueCodecManager,
      final Map<Integer, ColumnReference> columnReferenceMapByIndex,
      final Map<ColumnIdentifier, ColumnReference> columnReferenceMapByIdentifier
  ) {
    final int columnReferenceCount = byteBuffer.getInt();
    for (int index = 0; index < columnReferenceCount; index++) {
      final ColumnReference columnReference = decodeColumn(byteBuffer, decodeContext, valueCodecManager);
      final ColumnIdentifier columnIdentifier = new ColumnIdentifier(
          decodeIdentifier(byteBuffer).getBytes(),// Column family.
          decodeIdentifier(byteBuffer).getBytes() // Column qualifier.
      );
      columnReferenceMapByIndex.put(columnReference.getIndexInTable(), columnReference);
      columnReferenceMapByIdentifier.put(columnIdentifier, columnReference);
    }
  }

  private static ColumnReference decodeColumn(
      final ByteBuffer byteBuffer,
      final ExpressionCodec.DecodeContext decodeContext,
      final HBaseValueCodecManager valueCodecManager
  ) {
    final int columnIndexInTable = byteBuffer.getInt();
    final HBaseValueCodec columnCodec = valueCodecManager.getValueCode(byteBuffer.getShort());
    final boolean columnNullable = decodeBoolean(byteBuffer);
    final Expression columnDefaultValue = ExpressionCodec.decode(decodeContext, byteBuffer);
    return new ColumnReference(
        columnIndexInTable,
        columnCodec,
        (Evaluation) columnDefaultValue,
        columnNullable
    );
  }

  private static boolean decodeBoolean(final ByteBuffer byteBuffer) {
    switch (byteBuffer.get()) {
      case VALUE_TRUE:
        return true;
      case VALUE_FALSE:
        return false;
      default:
        throw new RuntimeException();
    }
  }

  private static String decodeString(final ByteBuffer byteBuffer) {
    return STRING_CODEC.decodeWithLength(byteBuffer);
  }

  private static HBaseIdentifier decodeIdentifier(final ByteBuffer byteBuffer) {
    return HBaseIdentifier.readWithLength(byteBuffer);
  }

  private static final class AttributeIndexCollector extends ExpressionVisitor<Void, Set<Integer>> {

    private static final AttributeIndexCollector INSTANCE = new AttributeIndexCollector();

    static void collect(final Expression expression, final Set<Integer> attributeIndexes) {
      INSTANCE.collect0(expression, attributeIndexes);
    }

    private void collect0(final Expression expression, final Set<Integer> attributeIndexes) {
      if (expression != null) {
        expression.accept(this, attributeIndexes);
      }
    }

    @Override
    public Void visit(final Casting<?, ?> casting, final Set<Integer> attributeIndexes) {
      this.collect0(casting.getSource(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(final Constant<?> constant, final Set<Integer> attributeIndexes) {
      return null;
    }

    @Override
    public Void visit(final Reference<?> reference, final Set<Integer> attributeIndexes) {
      attributeIndexes.add(reference.getAttributeIndex());
      return null;
    }

    @Override
    public Void visit(final Variable<?> variable, final Set<Integer> attributeIndexes) {
      return null;
    }

    @Override
    public Void visit(final LikeExpression like, final Set<Integer> attributeIndexes) {
      this.collect0(like.getValueParameter(), attributeIndexes);
      this.collect0(like.getPatternParameter(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(final LogicalAnd logicalAnd, final Set<Integer> attributeIndexes) {
      this.collect0(logicalAnd.getParameter1(), attributeIndexes);
      this.collect0(logicalAnd.getParameter2(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(Case caze, Set<Integer> attributeIndexes) {
      List<Case.Branch<?>> branches = caze.getBranches();
      for (Case.Branch<?> branch : branches){
        this.collect0(branch.getCondition(), attributeIndexes);
        this.collect0(branch.getOperate(), attributeIndexes);
      }
      this.collect0(caze.getOperateElse(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(LogicalXor logicalXor, Set<Integer> attributeIndexes) {
      this.collect0(logicalXor.getParameter1(), attributeIndexes);
      this.collect0(logicalXor.getParameter2(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(final LogicalNot logicalNot, final Set<Integer> attributeIndexes) {
      this.collect0(logicalNot.getParameter(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(final LogicalOr logicalOr, final Set<Integer> attributeIndexes) {
      this.collect0(logicalOr.getParameter1(), attributeIndexes);
      this.collect0(logicalOr.getParameter2(), attributeIndexes);
      return null;
    }

    @Override
    public Void visit(final Function<?> function, final Set<Integer> attributeIndexes) {
      final int functionParameterCount = function.getParameterCount();
      for (int index = 0; index < functionParameterCount; index++) {
        this.collect0(function.getParameter(index), attributeIndexes);
      }
      return null;
    }

  }

  private static final class HBaseTablePrimaryKey extends HBaseTableReferencePart.PrimaryKey {

    HBaseTablePrimaryKey(
        final HBaseRowKeyCodecType codecType,
        final ColumnReference[] primaryKeyColumnList,
        final boolean[] primaryKeyColumnUsedMarkList
    ) {
      super(codecType, new HBaseRowKeyDefinition() {
        @Override
        public boolean isTheLastColumn(final int columnIndex) {
          return primaryKeyColumnList.length == columnIndex + 1;
        }

        @Override
        public boolean isColumnNullable(final int columnIndex) {
          return primaryKeyColumnList[columnIndex].isNullable();
        }

        @Override
        public int getColumnCount() {
          return primaryKeyColumnList.length;
        }

        @Override
        public int getColumnIndexInTable(final int columnIndex) {
          return primaryKeyColumnList[columnIndex].getIndexInTable();
        }

        @Override
        public HBaseValueCodec getColumnCodec(final int columnIndex) {
          return primaryKeyColumnList[columnIndex].getCodec();
        }
      });
      this.primaryKeyColumnList = primaryKeyColumnList;
      this.primaryKeyColumnUsedMarkList = primaryKeyColumnUsedMarkList;
    }

    final ColumnReference[] primaryKeyColumnList;

    final boolean[] primaryKeyColumnUsedMarkList;

  }

}
