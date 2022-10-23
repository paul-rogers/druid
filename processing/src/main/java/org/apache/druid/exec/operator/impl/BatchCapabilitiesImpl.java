package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.BatchCapabilities;

public class BatchCapabilitiesImpl implements BatchCapabilities
{
  private final boolean canSeek;
  private final boolean canSort;

  public BatchCapabilitiesImpl(
      final boolean canSeek,
      final boolean canSort
  )
  {
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
}
