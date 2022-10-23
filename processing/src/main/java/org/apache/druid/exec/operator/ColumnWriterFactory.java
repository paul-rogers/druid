package org.apache.druid.exec.operator;

import org.apache.druid.exec.operator.RowSchema.ColumnSchema;

public interface ColumnWriterFactory
{
  public interface ScalarColumnWriter
  {
    ColumnSchema schema();
    void setNull();
    void setString(String value);
    void setLong(long value);
    void setDouble(double value);
    void setObject(Object value);

    /**
     * Set the value of any column type from an Object of that type.
     * This operation does conversions and is expensive. Use the other
     * methods when types are known.
     */
    void setValue(Object value);
  }

  RowSchema schema();
  ScalarColumnWriter scalar(String name);
  ScalarColumnWriter scalar(int ordinal);
}
