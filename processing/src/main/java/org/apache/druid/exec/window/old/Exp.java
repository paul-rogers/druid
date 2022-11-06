package org.apache.druid.exec.window.old;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.window.BatchBuffer;

import java.util.Comparator;

public class Exp
{
  // TODO: Legacy nulls wrapper

  public static class WindowDefinition
  {
    private int maxLag;
    private int maxLead;
    // Offset groups
  }

  public interface WindowState
  {

  }

  public static class PartitionTracker
  {
    private final BatchBuffer buffer;
    private final BatchType batchType;
    private final BatchReader reader;
    private final ScalarColumnReader[] keyColumns;
    private final Comparator<Object>[] comparators;
    private final Object[] currentKey;
    private boolean eof;

    // The current (lead-most) batch.
    private int batchIndex;
    private int rowIndex;

    // The start of the current partition, perhaps on a previous batch.
    private int startBatch = -1;
    private int startOffset = -1;

    // The end of the partition within the current batch. If the same as
    // the batch size, then the partition may continue into the next batch.
    private int endOffset = -1;

    public void start()
    {
      batchIndex = 0;
      rowIndex = 0;
      if (loadBatch()) {
        loadGroupKey();
        findGroupEndWithinBatch();
      }
    }

    private void loadGroupKey()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        currentKey[i] = keyColumns[i].getValue();
      }
    }

    private void findGroupEndWithinBatch()
    {
      int low = rowIndex;
      int high = reader.size() - 1;
      while (low <= high) {
        rowIndex = (high + low) / 2;
        int result = compareKeys();
        if (result > 0) {
          high = rowIndex - 1;
        } else {
          low = rowIndex + 1;
        }
      }
      endOffset = high;
    }

    private int compareKeys()
    {
      for (int i = 0; i < keyColumns.length; i++) {
        int result = comparators[i].compare(currentKey[i], keyColumns[i].getValue());
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }

    public boolean nextBatch()
    {
      batchIndex++;
      rowIndex = 0;
      if (loadBatch()) {
        findGroupEndWithinBatch();
        return true;
      } else {
        return false;
      }
    }

    private boolean loadBatch()
    {
      Object data = buffer.loadBatch(batchIndex);
      if (data == null) {
        eof = true;
        return false;
      } else {
        batchType.bindReader(reader, data);
        return true;
      }
    }

    public boolean nextPartition()
    {
      if (eof) {
        return false;
      }

      // We cannot have found the end of the current partition if the
      // current partition extends to the end of the batch. Instead,
      // we must be at least one row away from the end.
      Preconditions.checkState(endOffset < reader.size());
      startBatch = batchIndex;
      startOffset = endOffset + 1;
      findGroupEndWithinBatch();
      return true;
    }

    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return batchIndex <= batchIndex && rowIndex <= endOffset;
    }

    public boolean isEOF()
    {
      return eof;
    }
  }
}
