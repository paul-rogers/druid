package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ColumnDefn.ResolvedColumn;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.MeasureTypes;
import org.apache.druid.catalog.model.table.MeasureTypes.MeasureType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;

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
      return DatasourceDefn.MEASURE_TYPE.equals(column.spec().type());
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
