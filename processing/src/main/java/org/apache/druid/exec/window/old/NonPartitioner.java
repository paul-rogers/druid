package org.apache.druid.exec.window.old;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchPositioner;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.window.ProjectionBuilder;

public class NonPartitioner extends BasePartitioner
{
  public static class Bounds implements PartitionBounds
  {
    @Override
    public int startBatch()
    {
      return 0;
    }

    @Override
    public int startRow()
    {
      return -1;
    }

    @Override
    public boolean isWithinPartition(int batchIndex, int rowIndex)
    {
      return true;
    }

    @Override
    public int seek(BatchPositioner positioner, int n)
    {
      int batchSize = positioner.size() - positioner.index() - 1;
      if (n < batchSize) {
        positioner.seek(positioner.index() + n);
        return n;
      } else {
        positioner.seek(positioner.size());
        return batchSize;
      }
    }
  }

  @VisibleForTesting
  public static final PartitionBounds GLOBAL_BOUNDS = new Bounds();

  public NonPartitioner(ProjectionBuilder builder, BatchWriter<?> writer)
  {
    super(builder, writer);
    for (WindowFrameCursor cursor : sequencer.cursors()) {
      cursor.bindPartitionBounds(GLOBAL_BOUNDS);
    }
    sequencer.startPartition();
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
    }
    return rowCount;
  }
}
