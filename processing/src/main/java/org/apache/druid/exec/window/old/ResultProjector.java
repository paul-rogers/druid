package org.apache.druid.exec.window.old;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.window.BatchBuffer;

public abstract class ResultProjector
{
  protected final BatchBuffer batchBuffer;
  protected final BatchWriter<?> writer;
  protected final WindowSpec spec;

  public ResultProjector(BatchBuffer batchBuffer, BatchWriter<?> writer, WindowSpec spec)
  {
    this.batchBuffer = batchBuffer;
    this.writer = writer;
    this.spec = spec;
  }

  public abstract int writeBatch();

  protected abstract boolean isEOF();
}
