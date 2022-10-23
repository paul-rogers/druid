package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.ColumnWriterFactory;
import org.apache.druid.exec.operator.RowSchema;

public class ColumnWriterFactoryImpl implements ColumnWriterFactory
{
  protected final RowSchema schema;
  protected final ScalarColumnWriter[] columnWriters;

  public ColumnWriterFactoryImpl(
      final RowSchema schema, 
      final ScalarColumnWriter[] columnWriters
  )
  {
    this.schema = schema;
    this.columnWriters = columnWriters;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public ScalarColumnWriter scalar(String name)
  {
    return scalar(schema.ordinal(name));
  }

  @Override
  public ScalarColumnWriter scalar(int ordinal)
  {
    return columnWriters[ordinal];
  }
}
