package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.druid.sql.calcite.table.DatasourceTable;

import java.util.ArrayList;
import java.util.List;

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
    SqlNode source = call.getSource();
    RelNode sourceRel = convertQueryRecursive(source, true, targetRowType).project();

    // If, however, the source is not SELECT, then we have to add on the clustering as
    // an additional ORDER BY clause.
    SqlValidatorNamespace insertNs;
    if (!(source instanceof SqlSelect)) {
      insertNs = validator.getNamespace(call);
      SqlValidatorTable target = insertNs.resolve().getTable();
      DatasourceTable dsTable = target.unwrap(DatasourceTable.class);
      SqlNodeList clusterBy = computeClusteredBy(call, dsTable);
      final List<RelFieldCollation> orderKeys = new ArrayList<>();
      for (SqlNode order : clusterBy) {
        final RelFieldCollation.Direction direction;
        switch (order.getKind()) {
        case DESCENDING:
          direction = RelFieldCollation.Direction.DESCENDING;
          order = ((SqlCall) order).operand(0);
          break;
        case NULLS_FIRST:
        case NULLS_LAST:
          throw new AssertionError();
        default:
          direction = RelFieldCollation.Direction.ASCENDING;
          break;
        }
        final RelFieldCollation.NullDirection nullDirection =
            validator.getDefaultNullCollation().last(desc(direction))
                ? RelFieldCollation.NullDirection.LAST
                : RelFieldCollation.NullDirection.FIRST;
        RexNode e = matchBb.convertExpression(order);
        orderKeys.add(
            new RelFieldCollation(((RexInputRef) e).getIndex(), direction,
                nullDirection));
      }
      final RelCollation collation = cluster.traitSet().canonize(RelCollations.of(orderKeys));
      sourceRel = LogicalSort.create(
          sourceRel,
          collation,
          null,
          null
          );
    }

    // We omit the column mapping and insert node that Calcite normally provides.
    // Presumably MSQ does these its own way.
    return sourceRel;
  }
}
