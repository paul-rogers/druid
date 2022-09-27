package org.apache.druid.catalog.specs;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CatalogUtils
{
  // Amazing that a parser doesn't already exist...
  private static final Map<String, Granularity> GRANULARITIES = new HashMap<>();

  static {
    GRANULARITIES.put("millisecond", Granularities.SECOND);
    GRANULARITIES.put("second", Granularities.SECOND);
    GRANULARITIES.put("minute", Granularities.MINUTE);
    GRANULARITIES.put("5 minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("5 minutes", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("five_minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("10 minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("10 minutes", Granularities.TEN_MINUTE);
    GRANULARITIES.put("ten_minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("15 minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("15 minutes", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("fifteen_minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("30 minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("30 minutes", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("thirty_minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("hour", Granularities.HOUR);
    GRANULARITIES.put("6 hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("6 hours", Granularities.SIX_HOUR);
    GRANULARITIES.put("six_hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("day", Granularities.DAY);
    GRANULARITIES.put("week", Granularities.WEEK);
    GRANULARITIES.put("month", Granularities.MONTH);
    GRANULARITIES.put("quarter", Granularities.QUARTER);
    GRANULARITIES.put("year", Granularities.YEAR);
    GRANULARITIES.put("all", Granularities.ALL);
  }

  public static Granularity toGranularity(String value)
  {
    return GRANULARITIES.get(StringUtils.toLowerCase(value));
  }

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
    Granularity gran = toGranularity(value);
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

  /**
   * {@code String}-to-{@code List<String>} conversion. The string can contain zero items,
   * one items, or a list. The list items are separated by a comma and optional
   * whitespace.
   */
  public static List<String> stringToList(String value)
  {
    if (value == null) {
      return null;
    }
    return Arrays.asList(((String) value).split(",\\s*"));
  }

  public static <T> T safeCast(Object value, Class<T> type, String propertyName)
  {
    if (value == null) {
      return null;
    }
    try {
      return type.cast(value);
    }
    catch (ClassCastException e)
    {
      throw new IAE("Value [%s] is not valid for property %s, expected type %s",
          value,
          propertyName,
          type.getSimpleName()
      );
    }
  }
}
