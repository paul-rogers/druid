package org.apache.druid.catalog;

import com.google.common.base.Strings;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalTableBuilder
{
  private TableDefn defn;
  private Map<String, Object> properties = new HashMap<>();
  private List<ColumnSpec> columns = new ArrayList<>();

  public ExternalTableBuilder(TableDefn defn)
  {
    this.defn = defn;
  }

  public static ExternalTableBuilder from(ResolvedTable table)
  {
    return new ExternalTableBuilder(table.defn())
        .properties(table.spec().properties())
        .columns(table.spec().columns());
  }

  public ExternalTableBuilder properties(Map<String, Object> properties)
  {
    this.properties = properties;
    return this;
  }

  public ExternalTableBuilder property(String key, Object value)
  {
    properties.put(key, value);
    return this;
  }

  public Map<String, Object> properties()
  {
    return properties;
  }

  public List<ColumnSpec> columns()
  {
    return columns;
  }

  public ExternalTableBuilder column(ColumnSpec column)
  {
    if (Strings.isNullOrEmpty(column.name())) {
      throw new IAE("Column name is required");
    }
    columns.add(column);
    return this;
  }

  public ExternalTableBuilder column(String name, String sqlType)
  {
    return column(new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, name, sqlType, null));
  }

  public ExternalTableBuilder columns(List<ColumnSpec> columns)
  {
    for (ColumnSpec col : columns) {
      column(new ColumnSpec(col));
    }
    return this;
  }

  public TableSpec build()
  {
    return new TableSpec(
        defn.typeValue(),
        properties,
        columns
    );
  }
}
