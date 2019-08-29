package com.huawei.cloudtable.leo.hbase.filters;

import com.huawei.cloudtable.leo.*;
import com.huawei.cloudtable.leo.HBaseTableReferencePart.PrimaryKey;
import com.huawei.cloudtable.leo.HBaseTableReferencePart.ColumnIdentifier;
import com.huawei.cloudtable.leo.HBaseTableReferencePart.ColumnReference;
import com.huawei.cloudtable.leo.common.CollectionHelper;
import com.huawei.cloudtable.leo.expression.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

public final class ConditionFilter extends FilterBase {

  public static Filter parseFrom(final byte[] bytes) {
    return Decoder.decode(bytes);
  }

  public ConditionFilter(
      final HBaseTableReference tableReference,
      final Evaluation<Boolean> condition
  ) {
    if (tableReference == null) {
      throw new IllegalArgumentException("Argument [tableReference] is null.");
    }
    if (condition == null) {
      throw new IllegalArgumentException("Argument [condition] is null.");
    }
    this.tableReference = tableReference;
    this.tableReferencePart = null;
    this.condition = condition;
    this.runtimeContext = null;
    this.primaryKeyColumnList = null;
    this.primaryKeyDecoder = null;
    this.columnIdentifier = null;
  }

  @SuppressWarnings("unchecked")
  private ConditionFilter(
      final HBaseTableReferencePart tableReferencePart,
      final Evaluation<Boolean> condition
  ) {
    final CompileContext compileContext = new CompileContext(Collections.emptySet());// TODO hints.
    condition.compile(compileContext);
    // TODO compile all column default value.
    final PrimaryKey primaryKey = tableReferencePart.getPrimaryKey();
    final ColumnReference[] primaryKeyColumnList;
    final HBaseRowKeyCodec.Decoder primaryKeyDecoder;
    if (primaryKey == null) {
      primaryKeyColumnList = null;
      primaryKeyDecoder = null;
    } else {
      final HBaseRowKeyDefinition primaryKeyDefinition = primaryKey.getDefinition();
      final int primaryKeyColumnCount = primaryKeyDefinition.getColumnCount();
      primaryKeyColumnList = new ColumnReference[primaryKeyColumnCount];
      for (int index = 0; index < primaryKeyColumnCount; index++) {
        primaryKeyColumnList[index] = tableReferencePart.getColumn(primaryKeyDefinition.getColumnIndexInTable(index));
      }
      if (CollectionHelper.isAllNull(primaryKeyColumnList)) {
        primaryKeyDecoder = null;
      } else {
        primaryKeyDecoder = primaryKey.getCodecType().newCodec(primaryKey.getDefinition()).decoder();
      }
    }
    this.tableReference = null;
    this.tableReferencePart = tableReferencePart;
    this.condition = condition;
    this.runtimeContext = new RuntimeContext(tableReferencePart);// TODO 考虑重用
    this.primaryKeyColumnList = primaryKeyDecoder == null ? null : primaryKeyColumnList;
    this.primaryKeyDecoder = primaryKeyDecoder;
    this.columnIdentifier = new ColumnIdentifier();
  }

  private final HBaseTableReference tableReference;

  private final HBaseTableReferencePart tableReferencePart;

  private final Evaluation<Boolean> condition;

  private final RuntimeContext runtimeContext;

  private final ColumnReference[] primaryKeyColumnList;

  private final HBaseRowKeyCodec.Decoder primaryKeyDecoder;

  private final ColumnIdentifier columnIdentifier;

  private Boolean filter;

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean filterRowKey(final Cell cell) {
    if (cell.getRowLength() == 0) {
      return true;
    }
    // Reset.
    this.filter = null;
    this.runtimeContext.reset();
    //
    if (this.primaryKeyDecoder != null) {
      this.primaryKeyDecoder.setRowKey(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
      for (int index = 0; index < this.primaryKeyColumnList.length; index++) {
        this.primaryKeyDecoder.next();
        final HBaseTableReferencePart.ColumnReference primaryKeyColumn = this.primaryKeyColumnList[index];
        if (primaryKeyColumn != null) {
          this.runtimeContext.setAttribute(primaryKeyColumn.getIndexInTable(), this.primaryKeyDecoder.getAsBytes());
        }
      }
      if (this.runtimeContext.isAllAttributeSet()) {
        if (this.filter(false)) {
          return true;
        } else {
          this.filter = false;
        }
      }
    }
    return false;
  }

  @Override
  public ReturnCode filterCell(final Cell cell) {
    if (this.filter == null) {
      this.columnIdentifier.set(
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()
      );
      final ColumnReference column = this.tableReferencePart.getColumn(this.columnIdentifier);
      if (column != null) {
        this.runtimeContext.setAttribute(
            column.getIndexInTable(),
            ValueBytes.of(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
        );
        if (this.runtimeContext.isAllAttributeSet()) {
          if (this.filter(false)) {
            this.filter = true;
            return ReturnCode.NEXT_ROW;
          } else {
            this.filter = false;
          }
        }
      }
    }
    return ReturnCode.INCLUDE_AND_NEXT_COL;
  }

  @Override
  public void filterRowCells(List<Cell> ignored) {
    if (!this.runtimeContext.isAllAttributeSet()) {
      this.filter = this.filter(true);
    }
  }

  @Override
  public boolean filterRow() {
    return this.filter;
  }

  private boolean filter(final boolean canUseDefaultValue) {
    try {
      return !this.condition.evaluate(this.runtimeContext);
    } catch (EvaluateException exception) {
      // TODO
      throw new UnsupportedOperationException(exception);
    }
  }

  @Override
  public byte[] toByteArray() {
    return Encoder.encode(this);
  }

  private static final class Encoder {

    private static final ThreadLocal<ByteBuffer> BYTE_BUFFER_CACHE = new ThreadLocal<ByteBuffer>() {
      @Override
      protected ByteBuffer initialValue() {
        return ByteBuffer.allocate(10240); // TODO
      }
    };

    private static final ExpressionCodec.EncodeContext ENCODE_CONTEXT = new ExpressionCodec.EncodeContext() {
      @Override
      public ValueCodecManager getValueCodecManager() {
        return HBaseValueCodecManager.BUILD_IN;
      }
    };

    static byte[] encode(final ConditionFilter conditionFilter) {
      final ByteBuffer byteBuffer = BYTE_BUFFER_CACHE.get();
      try {
        HBaseTableReferenceCodec.encode(conditionFilter.tableReference, conditionFilter.condition, ENCODE_CONTEXT, byteBuffer);
        ExpressionCodec.encode(ENCODE_CONTEXT, conditionFilter.condition, byteBuffer);
        final byte[] bytes = new byte[byteBuffer.position()];
        byteBuffer.position(0);
        byteBuffer.get(bytes);
        return bytes;
      } finally {
        byteBuffer.clear();
      }
    }

  }

  private static final class Decoder {

    private static final ExpressionCodec.DecodeContext DECODE_CONTEXT = new ExpressionCodec.DecodeContext() {
      @Override
      public ValueCodecManager getValueCodecManager() {
        return HBaseValueCodecManager.BUILD_IN;
      }

      @Override
      public FunctionManager getFunctionManager() {
        return FunctionManager.BUILD_IN;
      }
    };

    @SuppressWarnings("unchecked")
    static ConditionFilter decode(final byte[] bytes) {
      final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      final HBaseTableReferencePart tableReferencePart = HBaseTableReferenceCodec.decode(byteBuffer, DECODE_CONTEXT, HBaseValueCodecManager.BUILD_IN);
      final Expression condition = ExpressionCodec.decode(DECODE_CONTEXT, byteBuffer);
      return new ConditionFilter(tableReferencePart, (Evaluation<Boolean>) condition);
    }

  }

  private static final class CompileContext implements Evaluation.CompileContext {

    CompileContext(final Set<String> hints) {
      this.hints = hints;
    }

    private final Set<String> hints;

    @Override
    public Charset getCharset() {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> ValueType<TValue> getValueType(final Class<TValue> valueClass) {
      return ValueTypeManager.BUILD_IN.getValueType(valueClass);
    }

    @Override
    public <TValue> HBaseValueCodec<TValue> getValueCodec(final Class<TValue> valueClass) {
      return HBaseValueCodecManager.BUILD_IN.getValueCodec(valueClass);
    }

    @Override
    public boolean hasHint(final String hintName) {
      return this.hints.contains(hintName);
    }

  }

  private static final class RuntimeContext implements Evaluation.RuntimeContext {

    RuntimeContext(final HBaseTableReferencePart tableReference) {
      this.tableReference = tableReference;
      this.attributeValueMap = new HashMap<>(tableReference.getColumnCount());
      this.attributeValueBytesMap = new HashMap<>(tableReference.getColumnCount());
    }

    private final HBaseTableReferencePart tableReference;

    private final Map<Integer, Object> attributeValueMap;

    private final Map<Integer, ValueBytes> attributeValueBytesMap;

    @Override
    public <TValue> TValue getVariable(final Variable<TValue> variable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> ValueBytes getVariableAsBytes(final Variable<TValue> variable, final ValueCodec<TValue> valueCodec) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <TValue> TValue getAttribute(final Reference<TValue> reference) {
      Object attributeValue = this.attributeValueMap.get(reference.getAttributeIndex());
      if (attributeValue == null) {
        final ValueBytes attributeValueBytes = this.attributeValueBytesMap.get(reference.getAttributeIndex());
        if (attributeValueBytes == null) {
          return null;
        }
        final HBaseValueCodec<?> attributeCodec = this.tableReference.getColumn(reference.getAttributeIndex()).getCodec();
        attributeValue = attributeValueBytes.decode(attributeCodec);
        this.attributeValueMap.put(reference.getAttributeIndex(), attributeValue);
      }
      return reference.getResultClass().cast(attributeValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <TValue> ValueBytes getAttributeAsBytes(final Reference<TValue> reference, final ValueCodec<TValue> valueCodec) {
      final ValueBytes attributeValueBytes = this.attributeValueBytesMap.get(reference.getAttributeIndex());
      if (attributeValueBytes == null) {
        return null;
      }
      final HBaseValueCodec<?> attributeCodec = this.tableReference.getColumn(reference.getAttributeIndex()).getCodec();
      if (valueCodec == attributeCodec) {
        return attributeValueBytes;
      } else {
        return ValueBytes.of(valueCodec.encode((TValue) attributeValueBytes.decode(attributeCodec)));
      }
    }

    boolean isAllAttributeSet() {
      return this.attributeValueBytesMap.size() == this.tableReference.getColumnCount();
    }

    void setAttribute(final int attributeIndex, final ValueBytes attributeBytes) {
      this.attributeValueBytesMap.put(attributeIndex, attributeBytes);
    }

    void reset() {
      this.attributeValueMap.clear();
      this.attributeValueBytesMap.clear();
    }

  }

}
