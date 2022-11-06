package org.apache.druid.exec.window.old;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.window.ProjectionBuilder;

public abstract class BasePartitioner implements Partitioner
{
  protected final WindowFrameSequencer sequencer;
  protected final BatchWriter<?> writer;
  protected final RowWriter rowWriter;

  public BasePartitioner(ProjectionBuilder builder, BatchWriter<?> writer)
  {
    this.sequencer = builder.build();
    this.writer = writer;
    this.rowWriter = writer.rowWriter(builder.columnReaders());
  }

  @Override
  public boolean isEOF()
  {
    return sequencer.isEOF();
  }
}
