package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderFactory;

public abstract class BaseDirectReader extends AbstractBatchReader
{
  protected ColumnReaderFactory columnReaders;

  public BaseDirectReader(BatchSchema factory)
  {
    super(factory);
  }

  @Override
  public ColumnReaderFactory columns()
  {
    return columnReaders;
  }
}
