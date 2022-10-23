package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;

public class IndirectBatch implements Batch
{
  private final Batch base;
  private final int[] index;

  public IndirectBatch(final Batch base, final int[] index)
  {
    this.base = base;
    this.index = index;
  }

  @Override
  public BatchCapabilities capabilities()
  {
    return BatchCapabilities.IN_MEMORY_BATCH;
  }

  @Override
  public BatchReader newReader()
  {
    IndirectBatchReader reader = new IndirectBatchReader();
    reader.bind(base, index);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof IndirectBatchReader)) {
      return newReader();
    }
    ((IndirectBatchReader) reader).bind(base, index);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return base.newWriter();
  }
}
