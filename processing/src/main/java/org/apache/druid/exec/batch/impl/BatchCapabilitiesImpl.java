package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.operator.BatchCapabilities;

public class BatchCapabilitiesImpl implements BatchCapabilities
{
  private final BatchFormat batchFormat;
  private final boolean canSeek;
  private final boolean canSort;

  public BatchCapabilitiesImpl(
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
}
