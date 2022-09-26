package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.InputColumnSpec;
import org.apache.druid.catalog.MeasureTypes;
import org.apache.druid.catalog.MeasureTypes.MeasureType;
import org.apache.druid.catalog.specs.ColumnDefn.ResolvedColumn;
import org.apache.druid.catalog.specs.Columns;
import org.apache.druid.catalog.specs.Constants;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

public class ColumnFacade
{
  public static class DatasourceColumnFacade extends ColumnFacade
  {
    public DatasourceColumnFacade(ResolvedColumn column)
    {
      super(column);
    }

    @Override
    public ColumnType druidType()
    {
      if (Columns.isTimeColumn(column.spec().name())) {
        return ColumnType.LONG;
      }
      return super.druidType();
    }

    public boolean isMeasure()
    {
      return Constants.MEASURE_TYPE.equals(column.spec().type());
    }

    public MeasureType measureType()
    {
      String sqlType = column.spec().sqlType();
      if (sqlType == null) {
        return null;
      }
      try {
        return MeasureTypes.parse(sqlType);
      }
      catch (ISE e) {
        return null;
      }
    }
  }

  public static class InputColumnFacade extends ColumnFacade
  {
    public RowSignature rowSignature()
    {
      List<ColumnSpec> columns = column.spec().c
      RowSignature.Builder builder = RowSignature.builder();
      if (columns() != null) {
        for (InputColumnSpec col : columns()) {
          ColumnType druidType = Columns.SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(col.sqlType()));
          if (druidType == null) {
            druidType = ColumnType.STRING;
          }
          builder.add(col.name(), druidType);
        }
      }
      return builder.build();
    }


  }

  protected final ResolvedColumn column;

  public ColumnFacade(ResolvedColumn column)
  {
    this.column = column;
  }

  public ColumnType druidType()
  {
    String sqlType = column.spec().sqlType();
    return sqlType == null ? null : Columns.druidType(sqlType);
  }
}
