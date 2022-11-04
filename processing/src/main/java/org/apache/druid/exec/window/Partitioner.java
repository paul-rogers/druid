package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchPositioner;

public interface Partitioner
{
  public interface PartitionBounds
  {
    int startBatch();
    int startRow();
    boolean isWithinPartition(int batchIndex, int rowIndex);
    int seek(BatchPositioner positioner, int n);
  }

  int writeBatch();
  boolean isEOF();
}
