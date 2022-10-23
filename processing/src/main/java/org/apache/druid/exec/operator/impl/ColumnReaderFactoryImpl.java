package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.ColumnReaderFactory;
import org.apache.druid.exec.operator.RowSchema;

public class ColumnReaderFactoryImpl implements ColumnReaderFactory
{
  public interface ColumnReaderMaker
  {
    ScalarColumnReader buildReader(int index);
  }

  private final RowSchema schema;
  private final ColumnReaderMaker readerMaker;
  private final ScalarColumnReader[] columnReaders;

  public ColumnReaderFactoryImpl(final RowSchema schema, final ColumnReaderMaker readerMaker)
  {
    this.schema = schema;
    columnReaders = new ScalarColumnReader[schema.size()];
    this.readerMaker = readerMaker;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public ScalarColumnReader scalar(String name)
  {
    int index = schema.ordinal(name);
    return index == -1 ? null : scalar(index);
  }

  @Override
  public ScalarColumnReader scalar(int ordinal)
  {
    if (columnReaders[ordinal] == null) {
      columnReaders[ordinal] = readerMaker.buildReader(ordinal);
    }
    return columnReaders[ordinal];
  }
}
