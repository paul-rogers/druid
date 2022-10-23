package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.ColumnReaderFactory;

public class IndirectBatchReader extends AbstractBatchReader
{
  private BatchReader baseReader;
  private int[] index;

  public void bind(final Batch base, final int[] index)
  {
    this.baseReader = base.bindReader(baseReader);
    this.index = index;
    cursor.bind(baseReader.cursor().size());
  }

  @Override
  protected void bindRow(int posn)
  {
    baseReader.cursor().seek(index[posn]);
  }

  @Override
  public ColumnReaderFactory columns()
  {
    return baseReader.columns();
  }
}
