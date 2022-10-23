package org.apache.druid.exec.util;

import com.google.common.collect.Ordering;
import org.apache.druid.exec.util.TypeRegistry.TypeAttributes;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;

import java.util.Comparator;

public class OrderingBuilder
{
  public static Comparator<Object> sortOrdering(SortColumn key, ColumnType type)
  {
    if (type == null) {
      throw new ISE("Sort key [%s]: input schema has no type", key.columnName());
    }
    TypeAttributes attribs = TypeRegistry.INSTANCE.resolve(type);
    if (attribs == null) {
      throw new ISE(
          "Sort key [%s]: type [%s] not found in the type registry",
          key.columnName(),
          type.asTypeString()
      );
    }
    Ordering<Object> ordering = attribs.objectOrdering();
    if (ordering == null) {
      throw new ISE(
          "Sort key [%s]: type [%s] is not orderable",
          key.columnName(),
          type.asTypeString()
      );
    }
    if (key.descending()) {
      ordering = ordering.reverse();
    }
    return ordering;
  }
}
