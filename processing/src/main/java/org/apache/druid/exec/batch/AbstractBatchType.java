package org.apache.druid.exec.batch;

import org.apache.druid.exec.batch.impl.BatchImpl;

public abstract class AbstractBatchType implements BatchType
{
  private final BatchFormat batchFormat;
  private final boolean canSeek;
  private final boolean canSort;

  public AbstractBatchType(
      final BatchFormat format,
      final boolean canSeek,
      final boolean canSort
  )
  {
    this.batchFormat = format;
    this.canSeek = canSeek;
    this.canSort = canSort;
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
