package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
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

  /**
   * Convert a Druid {@code INSERT} or {@code REPLACE} statement. The code is the same
   * as the normal conversion, except we don't actually create the final modify node.
   * Druid has its own special way to handle inserts. (This should probably change in
   * some future, but doing so requires changes in the SQL engine and MSQ, which is a bit
   * invasive.)
   */
  @Override
  protected RelNode convertInsert(SqlInsert call)
  {
    // Get the target type: the column types we want to write into the target datasource.
    final RelDataType targetRowType = validator.getValidatedNodeType(call);
    assert targetRowType != null;

    // Convert the underlying SELECT. We pushed the CLUSTERED BY clause into the SELECT
    // as its ORDER BY. We claim this is the top query because MSQ doesn't actually
    // use the Calcite insert node.
    RelNode sourceRel = convertQueryRecursive(call.getSource(), true, targetRowType).project();

    // We omit the column mapping and insert node that Calcite normally provides.
    // Presumably MSQ does these its own way.
    return sourceRel;
  }
}
