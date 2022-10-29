package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.RowSchema;

public abstract class AbstractBatchType implements BatchType
{
  private final BatchFormat batchFormat;
  private final boolean canSeek;
  private final boolean canSort;
  private final boolean canWrite;

  public AbstractBatchType(
      final BatchFormat format,
      final boolean canSeek,
      final boolean canSort,
      final boolean canWrite
  )
  {
    this.batchFormat = format;
    this.canSeek = canSeek;
    this.canSort = canSort;
    this.canWrite = canWrite;
  }

  @Override
  public boolean canWrite()
  {
    return canWrite;
  }

  @Override
  public boolean canSeek()
  {
    return canSeek;
  }

  @Override
  public boolean canSort()
  {
    return canSort;
  }

  @Override
  public BatchFormat format()
  {
    return batchFormat;
  }

  @Override
  public boolean canDirectCopyFrom(BatchType otherType)
  {
    // The hoped-for default. Revise if the type is more general
    // or restrictive.
    return this == otherType;
  }

  @Override
  public Batch newBatch(RowSchema schema)
  {
    return new BatchImpl(factory(schema));
  }

  @Override
  public BatchFactory factory(RowSchema schema)
  {
    return new BatchFactory(this, schema);
  }
}
