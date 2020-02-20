package org.apache.carbondata.core.datastore.page;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;

public abstract class UnsafeVarLengthColumnPageBase extends VarLengthColumnPageBase {

  // memory allocated by Unsafe
  protected MemoryBlock memoryBlock;

  // base address of memoryBlock
  protected Object baseAddress;

  // base offset of memoryBlock
  protected long baseOffset;

  // size of the allocated memory, in bytes
  protected int capacity;

  UnsafeVarLengthColumnPageBase(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
  }

  /**
   * reallocate memory if capacity length than current size + request size
   */
  protected void ensureMemory(int requestSize) {
    if (totalLength + requestSize > capacity) {
      int newSize = Math.max(2 * capacity, totalLength + requestSize);
      MemoryBlock newBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, newSize);
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
          newBlock.getBaseObject(), newBlock.getBaseOffset(), capacity);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = newBlock;
      baseAddress = newBlock.getBaseObject();
      baseOffset = newBlock.getBaseOffset();
      capacity = newSize;
    }
  }

}
