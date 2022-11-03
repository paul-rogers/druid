package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider;

public abstract class BaseDirectReader extends AbstractBatchReader
{
  protected ColumnReaderProvider columnReaders;

  public BaseDirectReader(BatchSchema schema)
  {
    super(schema);
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return columnReaders;
  }
}
