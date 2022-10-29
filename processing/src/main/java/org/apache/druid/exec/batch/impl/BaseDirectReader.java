package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.ColumnReaderFactory;

public abstract class BaseDirectReader extends AbstractBatchReader
{
  protected ColumnReaderFactory columnReaders;

  public BaseDirectReader(BatchFactory factory)
  {
    super(factory);
  }

  @Override
  public ColumnReaderFactory columns()
  {
    return columnReaders;
  }
}
