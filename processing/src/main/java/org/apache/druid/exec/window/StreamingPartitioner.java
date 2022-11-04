package org.apache.druid.exec.window;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.PositionListener;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.batch.impl.SimpleBatchPositioner;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.exec.window.BatchBufferOld.BufferPosition;
import org.apache.druid.exec.window.BatchBufferOld.InputReader;
import org.apache.druid.exec.window.BatchBufferOld.PartitionRange;

import java.util.Comparator;
import java.util.List;

public class StreamingPartitioner extends BasePartitioner
{
  private final WindowFrameCursor reader;
  private final ScalarColumnReader[] keyColumns;
  private final Comparator<Object>[] comparators;
  private final Object[] currentKey;
  private int startBatch;
  private int startOffset;
  private int endBatch;
  private int endOffset;

  public StreamingPartitioner(ProjectionBuilder builder, BatchWriter<?> writer, final List<String> keys)
  {
    super(builder, writer);
    Preconditions.checkArgument(!keys.isEmpty());
    List<WindowFrameCursor> cursors = sequencer.cursors();
    this.reader = cursors.get(0);
    this.reader.bindPartitionBounds(new LeadBounds());
    PartitionBounds followerBounds = new FollowerBounds();
    for (int i = 1; i < cursors.size(); i++) {
      cursors.get(i).bindPartitionBounds(followerBounds);
    }
    this.comparators = TypeRegistry.INSTANCE.ordering(keys, reader.columns().schema());
    this.keyColumns = new ScalarColumnReader[keys.size()];
    for (int i = 0; i < keyColumns.length; i++) {
      this.keyColumns[i] = reader.columns().scalar(keys.get(i));
    }
    this.currentKey = new Object[keyColumns.length];
    sequencer.startPartition();
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
        endBatch = reader.batchIndex;
        endOffset = reader.cursor.positioner().index();
        return false;
      }
    }
    return true;
  }

  public void markPartitionStart()
  {
    startBatch = reader.batchIndex;
    startOffset = reader.cursor.positioner().index();
  }

  public void startNextPartition()
  {
    startBatch = endBatch;
    startOffset = endOffset;
    endBatch = -1;
    endOffset = -1;
  }

  @Override
  public int writeBatch()
  {
    int rowCount = 0;
    while (!writer.isFull()) {
      while (sequencer.next()) {
        rowWriter.write();
        rowCount++;
      }
      if (reader.isEOF()) {
        break;
      }
      startNextPartition();
      sequencer.startPartition();
    }
    return rowCount;
  }

  private abstract class BaseBounds implements PartitionBounds
  {
    @Override
    public int startBatch()
    {
      return startBatch;
    }

    @Override
    public int startRow()
    {
      return startOffset;
    }
  }

  private class LeadBounds extends BaseBounds
  {
    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return isSamePartition();
    }
  }

  private class FollowerBounds extends BaseBounds
  {
    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return endBatch == -1 || (batchIndex <= endBatch && rowIndex <= endOffset);
    }
  }
}
