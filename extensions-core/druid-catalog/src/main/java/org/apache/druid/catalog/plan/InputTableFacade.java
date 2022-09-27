package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.specs.ColumnSpec;
import org.apache.druid.catalog.specs.Columns;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;

public class InputTableFacade extends TableFacade
{
  public InputTableFacade(ResolvedTable resolved)
  {
    super(resolved);
  }

  public RowSignature rowSignature()
  {
    List<ColumnSpec> columns = spec().columns();
    RowSignature.Builder builder = RowSignature.builder();
    for (ColumnSpec col : columns) {
      ColumnType druidType = Columns.SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(col.sqlType()));
      if (druidType == null) {
        druidType = ColumnType.STRING;
      }
      builder.add(col.name(), druidType);
    }
    return builder.build();
  }
}
