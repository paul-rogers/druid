package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider;

public abstract class BaseDirectCursor extends AbstractBatchCursor
{
  protected ColumnReaderProvider columnReaders;

  public BaseDirectCursor(BatchSchema schema, BindableRowPositioner positioner)
  {
    super(schema, positioner);
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return columnReaders;
  }
}
