package com.huawei.cloudtable.leo;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public enum HBaseRowKeyCodecType {

  ALIGNMENT() {
    @Override
    public void checkTableDefinition(final HBaseRowKeyDefinition rowKeyDefinition) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public HBaseRowKeyCodec newCodec(final HBaseRowKeyDefinition rowKeyDefinition) {
      this.checkTableDefinition(rowKeyDefinition);
      // TODO
      throw new UnsupportedOperationException();
    }
  },

  DELIMITER() {

    private static final byte ROW_KEY_DELIMITER = 0;// 如果不用0，则在多列主键的情况下，无法保证字节序与逻辑序一致，该模式下，只在部分场景下才能将主键当成组合索引用。

    @Override
    public void checkTableDefinition(final HBaseRowKeyDefinition rowKeyDefinition) {
      // Primary Key第一个字段和所有非最后一个字段，都不允许为null
      for (int columnIndex = 0; columnIndex < rowKeyDefinition.getColumnCount() - 1; columnIndex++) {
        if (rowKeyDefinition.isColumnNullable(columnIndex)
            && (columnIndex == 0 || rowKeyDefinition.isTheLastColumn(columnIndex))) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
    }

    @Override
    public HBaseRowKeyCodec newCodec(final HBaseRowKeyDefinition rowKeyDefinition) {
      this.checkTableDefinition(rowKeyDefinition);
      return new HBaseRowKeyCodec() {
        @Override
        public byte[] encode(final HBaseRowKeyValueIterator rowKeyValueIterator) {
          return this.encode(rowKeyValueIterator, rowKeyDefinition.getColumnCount());
        }

        @SuppressWarnings("unchecked")
        @Override
        public byte[] encode(final HBaseRowKeyValueIterator rowKeyValueIterator, final int rowKeyValueCount) {
          final ByteBuffer rowKeyBuffer = HBaseRowKeyCodecType.getByteBufferFromCache();
          ValueCodec thePreviousColumnCodec = null;
          for (int columnIndex = 0; columnIndex < rowKeyValueCount; columnIndex++) {
            final Object columnValue;
            if (rowKeyValueIterator.hasNext()) {
              rowKeyValueIterator.next();
              columnValue = rowKeyValueIterator.get();
            } else {
              break;// TODO 要验证一下，逻辑是否准确
            }
            if (columnValue == null) {
              if (columnIndex == 0 || !rowKeyDefinition.isTheLastColumn(columnIndex)) {
                // Primary Key第一个字段和所有非最后一个字段，都不允许为null
                throw new RuntimeException();
              }
            } else {
              if (thePreviousColumnCodec != null && !thePreviousColumnCodec.isFixedLength()) {
                rowKeyBuffer.put(ROW_KEY_DELIMITER);
              }
              thePreviousColumnCodec = rowKeyDefinition.getColumnCodec(columnIndex);// 提前赋值了
              if (thePreviousColumnCodec.isFixedLength()) {
                thePreviousColumnCodec.encode(columnValue, rowKeyBuffer);
              } else {
                int checkPosition = rowKeyBuffer.position();
                thePreviousColumnCodec.encode(columnValue, rowKeyBuffer);
                for (; checkPosition < rowKeyBuffer.position(); checkPosition++) {
                  if (rowKeyBuffer.get(checkPosition) == ROW_KEY_DELIMITER) {
                    // TODO
                    throw new UnsupportedOperationException();
                  }
                }
              }
            }
          }
          final byte[] rowKey = new byte[rowKeyBuffer.position()];
          rowKeyBuffer.position(0);
          rowKeyBuffer.get(rowKey);
          return rowKey;
        }

        @Override
        public Decoder decoder() {
          return new Decoder() {

            private byte[] rowKey;

            private int rowKeyEnd;

            private ValueCodec currentColumnCodec;

            private HBaseRowKeyValueBytes currentColumnValueBytes;

            private int currentColumnIndex = -1;

            private int currentColumnStart = -1;

            private int currentColumnEnd = -1;

            private int nextColumnStart = 0;

            @Override
            public void setRowKey(final byte[] rowKey) {
              this.setRowKey(rowKey, 0, rowKey.length);
            }

            @Override
            public void setRowKey(final byte[] rowKey, final int offset, final int length) {
              this.rowKey = rowKey;
              this.rowKeyEnd = offset + length;
              this.currentColumnCodec = null;
              this.currentColumnIndex = -1;
              this.currentColumnStart = -1;
              this.currentColumnEnd = -1;
              this.nextColumnStart = offset;
            }

            @Override
            public boolean hasNext() {
              return this.currentColumnIndex < rowKeyDefinition.getColumnCount() - 1;
            }

            @Override
            public void next() {
              if (this.hasNext()) {
                this.currentColumnIndex++;
                this.currentColumnCodec = rowKeyDefinition.getColumnCodec(this.currentColumnIndex);
                this.currentColumnStart = this.nextColumnStart;
                if (this.nextColumnStart < this.rowKeyEnd) {
                  if (this.currentColumnCodec.isFixedLength()) {
                    this.currentColumnEnd = this.currentColumnStart + this.currentColumnCodec.getFixedLength();
                    this.nextColumnStart = this.currentColumnEnd;
                  } else {
                    int index = this.currentColumnStart;
                    for (; index < this.rowKeyEnd; index++) {
                      if (this.rowKey[index] == ROW_KEY_DELIMITER) {
                        break;
                      }
                    }
                    this.currentColumnEnd = index;
                    this.nextColumnStart = this.currentColumnEnd + 1;
                  }
                }
              } else {
                throw new NoSuchElementException();
              }
            }

            @Override
            public Object get() {
              if (this.currentColumnIndex < 0) {
                throw new IllegalStateException();
              }
              if (this.currentColumnStart < this.rowKeyEnd) {
                final ByteBuffer byteBuffer = HBaseRowKeyCodecType.getByteBufferFromCache();
                byteBuffer.put(this.rowKey);
                byteBuffer.position(this.currentColumnStart);
                byteBuffer.limit(this.currentColumnEnd);
                return this.currentColumnCodec.decode(byteBuffer);
              } else {
                // All remain columns is null.
                return null;
              }
            }

            @Override
            public ValueBytes getAsBytes() {
              if (this.currentColumnIndex < 0) {
                throw new IllegalStateException();
              }
              if (this.currentColumnStart < this.rowKeyEnd) {
                if (this.currentColumnValueBytes == null) {
                  this.currentColumnValueBytes = new HBaseRowKeyValueBytes();
                }
                this.currentColumnValueBytes.set(this.rowKey, this.currentColumnStart, this.currentColumnEnd - this.currentColumnStart);
                return this.currentColumnValueBytes;
              } else {
                // All remain columns is null.
                return null;
              }
            }
          };
        }
      };
    }
  };

  public abstract void checkTableDefinition(final HBaseRowKeyDefinition rowKeyDefinition);

  public abstract HBaseRowKeyCodec newCodec(final HBaseRowKeyDefinition rowKeyDefinition);

  private static ByteBuffer getByteBufferFromCache() {
    final ByteBuffer byteBuffer = BYTE_BUFFER_CACHE.get();
    byteBuffer.position(0);
    byteBuffer.limit(byteBuffer.capacity());
    return byteBuffer;
  }

  private static final ThreadLocal<ByteBuffer> BYTE_BUFFER_CACHE = ThreadLocal.withInitial(() -> {
    return ByteBuffer.allocate(1024); // TODO from configuration. hbase.rowkey.maximumLength
  });

}
