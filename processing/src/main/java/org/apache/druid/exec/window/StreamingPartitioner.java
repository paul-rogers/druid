package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.PositionListener;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.batch.impl.SimpleBatchPositioner;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.exec.window.BatchBufferOld.BufferPosition;
import org.apache.druid.exec.window.BatchBufferOld.InputReader;
import org.apache.druid.exec.window.BatchBufferOld.PartitionRange;

import java.util.Comparator;
import java.util.List;

public class StreamingPartitioner
{
  public static class PartitionTracker
  {
    public int startBatch;
    public int startOffset;
    public int endBatch;
    public int endOffset;
  }

  private final WindowFrameCursor reader;
  private final ScalarColumnReader[] keyColumns;
  private final Comparator<Object>[] comparators;
  private final Object[] currentKey;
  private final PartitionTracker tracker = new PartitionTracker();
  private final PositionListener cursorListener;
  private boolean isRowValid;

  public StreamingPartitioner(final WindowFrameCursor reader, final List<String> keys)
  {
    Preconditions.checkArgument(!keys.isEmpty());
    this.reader = reader;
    this.cursorListener = reader.wrapListener(this);
    this.comparators = TypeRegistry.INSTANCE.ordering(keys, reader.columns().schema());
    this.keyColumns = new ScalarColumnReader[keys.size()];
    for (int i = 0; i < keyColumns.length; i++) {
      this.keyColumns[i] = reader.columns().scalar(keys.get(i));
    }
    this.currentKey = new Object[keyColumns.length];
  }

  public void startPartition()
  {
    for (int i = 0; i < keyColumns.length; i++) {
      currentKey[i] = keyColumns[i].getValue();
    }
  }

  public boolean isSamePartition()
  {
    for (int i = 0; i < keyColumns.length; i++) {
      if (comparators[i].compare(currentKey[i], keyColumns[i].getValue()) != 0) {
        tracker.endBatch = reader.batchIndex;
        tracker.endOffset = reader.cursor.positioner().index();
        return false;
      }
    }
    return true;
  }

  public void markPartitionStart()
  {
    tracker.startBatch = reader.batchIndex;
    tracker.startOffset = reader.cursor.positioner().index();
  }

  public void startNextPartition()
  {
    tracker.startBatch = tracker.endBatch;
    tracker.startOffset = tracker.endOffset;
    tracker.endBatch = -1;
    tracker.endOffset = -1;
    reader.seek(tracker.startBatch, tracker.startOffset);
  }
}
