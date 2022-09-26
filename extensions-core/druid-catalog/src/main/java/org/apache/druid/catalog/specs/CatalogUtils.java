package org.apache.druid.catalog.specs;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CatalogUtils
{
  /**
   * Merge of two maps. Uses a convention that if the value of an update is null,
   * then this is a request to delete the key in the merged map. Supports JSON
   * updates of the form <code>{"doomed": null, "updated": "foo"}</code>.
   */
  public static Map<String, Object> mergeMap(Map<String, Object> source, Map<String, Object> update)
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

  public static int findColumn(List<ColumnSpec> columns, String colName)
  {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(colName)) {
        return i;
      }
    }
    return -1;
  }

  public static List<String> columnNames(List<ColumnSpec> columns)
  {
    return columns
           .stream()
           .map(col -> col.name())
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

  /**
   * Convert a catalog granularity string to the Druid form. Catalog granularities
   * are either the usual descriptive strings (in any case), or an ISO period.
   * For the odd interval, the interval name is also accepted (for the other
   * intervals, the interval name is the descriptive string).
   */
  public static Granularity asDruidGranularity(String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return Granularities.ALL;
    }
    Granularity gran = Constants.toGranularity(value);
    if (gran != null) {
      return gran;
    }

    try {
      return new PeriodGranularity(new Period(value), null, null);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(StringUtils.format("%s is an invalid period string", value));
    }
  }

  public static boolean isDatasource(String tableType)
  {
    return Constants.DETAIL_DATASOURCE_TYPE.equals(tableType)
        || Constants.ROLLUP_DATASOURCE_TYPE.equals(tableType);
  }
}
