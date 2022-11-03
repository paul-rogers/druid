package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchPositioner;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderProvider;

public class SimpleBatchCursor implements BatchCursor
{
  private final BatchReader reader;
  private final BatchPositioner positioner;

  public SimpleBatchCursor(final BatchReader reader, final BatchPositioner positioner)
  {
    this.reader = reader;
    this.positioner = positioner;
    this.positioner.bindListener(reader);
    this.reader.bindListener(positioner);
  }

  public SimpleBatchCursor(final BatchReader reader)
  {
    this(reader, new SimpleBatchPositioner());
  }

  @Override
  public BatchPositioner positioner()
  {
    return positioner;
  }

  @Override
  public BatchReader reader()
  {
    return reader;
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return reader.columns();
  }
}
