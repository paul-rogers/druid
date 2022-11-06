package org.apache.druid.exec.window.old;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.window.BatchBuffer;

public class UnpartitionedProjector extends ResultProjector
{
  private WindowFrameSequencer sequencer;
  private RowWriter rowWriter;

  public UnpartitionedProjector(BatchBuffer batchBuffer, BatchWriter<?> writer, WindowSpec spec)
  {
    super(batchBuffer, writer, spec);
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

  @Override
  protected boolean isEOF()
  {
    return sequencer.isEOF();
  }
}
