package org.apache.druid.exec.util;

import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.RowSchema.ColumnSchema;
import org.apache.druid.exec.operator.impl.AbstractScalarReader;
import org.apache.druid.exec.operator.impl.BaseBatchReader;
import org.apache.druid.exec.operator.impl.ColumnReaderFactoryImpl;
import org.apache.druid.exec.operator.impl.ColumnReaderFactoryImpl.ColumnReaderMaker;

/**
 * Trivial reader for an empty batch. Most useful when the format of the batch
 * is unknown because every format behaves the same when empty.
 */
public class EmptyBatchReader<T> extends BaseBatchReader<T> implements ColumnReaderMaker
{
  private class ColumnReaderImpl extends AbstractScalarReader
  {
    private final int index;

    public ColumnReaderImpl(int index)
    {
      this.index = index;
    }

    @Override
    public ColumnSchema schema()
    {
      return columns().schema().column(index);
    }

    @Override
    public Object getObject()
    {
      return null;
    }
  }

  public EmptyBatchReader(RowSchema schema)
  {
    this.columnReaders = new ColumnReaderFactoryImpl(schema, this);
  }

  @Override
  public ScalarColumnReader buildReader(int index)
  {
    return new ColumnReaderImpl(index);
  }

  @Override
  protected void bindRow(int posn)
  {
  }

  @Override
  protected void reset()
  {
  }
}
