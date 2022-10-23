package org.apache.druid.exec.util;

public class ExecUtils
{
  public static <T> T unwrap(Object obj, Class<T> readerClass)
  {
    return obj.getClass() == readerClass ? readerClass.cast(obj) : null;
  }
}
