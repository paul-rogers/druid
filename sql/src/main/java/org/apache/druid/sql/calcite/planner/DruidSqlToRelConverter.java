package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class DruidSqlToRelConverter extends SqlToRelConverter
{
  public DruidSqlToRelConverter(
      final ViewExpander viewExpander,
      final SqlValidator validator,
      final CatalogReader catalogReader,
      RelOptCluster cluster,
      final SqlRexConvertletTable convertletTable,
      final Config config
  )
  {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

  @Override
  protected RelNode convertInsert(SqlInsert call)
  {
    RelNode sourceRel =
        convertQueryRecursive(call.getSource(), false, null).project();
    return sourceRel;
  }
}
