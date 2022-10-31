package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.RowSchema.ColumnSchema;

public class ConstantScalarReader extends AbstractScalarReader
{
  private final ColumnSchema schema;
  private final Object value;

  public ConstantScalarReader(ColumnSchema schema, Object value)
  {
    this.schema = schema;
    this.value = value;
  }

  @Override
  public ColumnSchema schema()
  {
    return schema;
  }

  @Override
  public Object getObject()
  {
    return value;
  }
}
