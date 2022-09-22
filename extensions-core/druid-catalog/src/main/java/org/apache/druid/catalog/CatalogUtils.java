package org.apache.druid.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CatalogUtils
{
  /**
   * Merge of two maps. Uses a convention that if the value of an update is null,
   * then this is a request to delete the key in the merged map. Supports JSON
   * updates of the form <code>{"doomed": null, "updated": "foo"}</code>.
   */
  protected static Map<String, Object> mergeMap(Map<String, Object> source, Map<String, Object> update)
  {
    if (update == null) {
      return source;
    }
    if (source == null) {
      return update;
    }
    Map<String, Object> tags = new HashMap<>(source);
    for (Map.Entry<String, Object> entry : update.entrySet()) {
      if (entry.getValue() == null) {
        tags.remove(entry.getKey());
      } else {
        tags.put(entry.getKey(), entry.getValue());
      }
    }
    return tags;
  }

  protected static <T extends ColumnSpec> List<T> mergeColumns(
      List<T> source,
      List<T> update,
      BiFunction<T, T, T> merger
  )
  {
    Map<String, Integer> original = new HashMap<>();
    for (int i = 0; i < source.size(); i++) {
      original.put(source.get(i).name(), i);
    }
    List<T> merged = new ArrayList<>(source);
    List<T> added = new ArrayList<>();
    for (T col : update) {
      Integer index = original.get(col.name);
      if (index == null) {
        added.add(col);
      } else {
        merged.set(index, merger.apply(merged.get(index), col));
      }
    }
    merged.addAll(added);
    return merged;
  }

  public static int findColumn(List<? extends ColumnSpec> columns, String colName)
  {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(colName)) {
        return i;
      }
    }
    return -1;
  }

  public static List<String> columnNames(List<? extends ColumnSpec> columns)
  {
    return columns
           .stream()
           .map(col -> col.name)
           .collect(Collectors.toList());
  }

  public static <T extends ColumnSpec> List<T> dropColumns(
      final List<T> columns,
      final List<String> toDrop)
  {
    if (toDrop == null || toDrop.isEmpty()) {
      return columns;
    }
    Set<String> drop = new HashSet<String>(toDrop);
    List<T> revised = new ArrayList<>();
    for (T col : columns) {
      if (!drop.contains(col.name())) {
        revised.add(col);
      }
    }
    return revised;
  }
}
