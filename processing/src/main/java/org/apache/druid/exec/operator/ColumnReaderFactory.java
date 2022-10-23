package org.apache.druid.exec.operator;

import org.apache.druid.exec.operator.RowSchema.ColumnSchema;

public interface ColumnReaderFactory
{
  public interface ScalarColumnReader
  {
    ColumnSchema schema();
    boolean isNull();
    String getString();
    long getLong();
    double getDouble();

    /**
     * Return the value of a complex object. This is not a generic
     * getter, for that use {@link #getValue()}.
     */
    Object getObject();

    /**
     * Return the value of any type, as a Java object. If the
     * column is null, returns {@code null}.
     */
    Object getValue();
  }

  RowSchema schema();
  ScalarColumnReader scalar(String name);
  ScalarColumnReader scalar(int ordinal);
}
